# Refactor pipeline Eloqua — Passage à une ingestion event-level (hit)

**Statut** : Proposition de conception — à valider
**Périmètre** : Tables `dw_echonet_eloqua.eloqua_campaigns_metrics` et `dw_echonet_eloqua.eloqua_campaigns_liens`
**Auteur** : Amar SEBAA
**Date** : Juillet 2026

---

## 1. Contexte et objectif

Aujourd'hui, les deux tables Gold `eloqua_campaigns_metrics` et `eloqua_campaigns_liens` sont construites à partir d'un appel API **au niveau campagne** : les métriques sont récupérées déjà agrégées côté source.

**Limite actuelle** : une campagne créée il y a plusieurs mois mais qui continue de générer des événements (opens, clicks tardifs) n'est pas remise à jour, car l'extraction ne re-visite pas les campagnes anciennes.

**Évolution** : 5 nouveaux endpoints event-level (hit) ont été mis à disposition :

| Endpoint | Contenu | Clé technique | Date d'événement |
|---|---|---|---|
| `mailing/campaigns/v1/email-send` | Envois | `emailSendID` | `sentDateHour` |
| `mailing/campaigns/v1/email-open` | Ouvertures (+ `firstOpen`, `deviceInfoId`) | `emailOpenID` | `openDateHour` |
| `mailing/campaigns/v1/email-bounceback` | Bounces (+ `bounceType`) | `emailBouncebackID` | `bounceDateHour` |
| `mailing/campaigns/v1/email-clickthrough` | Clics (+ `clickThroughLink`, `firstClick`, `baseUrlId`) | `emailClickthroughId` | `clickDateHour` |
| `mailing/campaigns/v1/email-asset` | Référentiel email (subject, catégorie, groupe) | `emailAssetID` / `emailID` | `lastModifiedAt` |

**Objectif** : reconstruire les deux tables Gold à partir de ces événements, afin que toute campagne recevant de nouveaux événements — quelle que soit sa date de création — voie ses métriques recalculées.

---

## 2. Analyse d'écart (gap analysis)

Champs des tables cibles vs disponibilité dans les endpoints :

| Champ cible | Table(s) | Disponible event-level ? | Source |
|---|---|---|---|
| `eloqua_campaign_id` | metrics + liens | ✅ | `eloquaCampaignID` (tous endpoints) |
| `campaign_name` | metrics + liens | ❌ | Appel campaign-level uniquement |
| `campaign_start_date` | metrics + liens | ❌ | Appel campaign-level uniquement |
| `total_sends` | metrics | ✅ | COUNT sur email-send |
| `total_opens` / `unique_opens` | metrics | ✅ | COUNT / DISTINCT sur email-open |
| `total_delivered` / `delivered_rate` | metrics | ✅ | sends − bounces |
| `total_clickthroughs` / rates | metrics | ✅ | COUNT sur email-clickthrough |
| `is_clickthroughed` | metrics | ✅ | Dérivé (total_clickthroughs > 0) |
| `clickthrough_link` | liens | ✅ | `clickThroughLink` |
| `ingestion_date` | metrics + liens | ✅ | Métadonnée pipeline |

> ⚠️ **Constat structurant** : `campaign_name` et `campaign_start_date` ne figurent dans **aucun** des 5 endpoints. C'est ce constat qui motive les deux options ci-dessous.

---

## 3. Option A — Endpoints events + appel campaign-level conservé comme référentiel

### 3.1 Principe

Les 5 endpoints deviennent la **source de vérité pour les faits** (événements). L'appel campaign-level existant est **conservé mais rétrogradé** au rôle de **dimension campagne** (référentiel : id, nom, date de début). Les métriques agrégées qu'il renvoie ne sont plus utilisées.

### 3.2 Architecture (Medallion)

```
Bronze (raw, partitionné par date d'événement)
├── raw_eloqua_email_send
├── raw_eloqua_email_open
├── raw_eloqua_email_bounceback
├── raw_eloqua_email_clickthrough
├── raw_eloqua_email_asset          (full ou incrémental sur lastModifiedAt)
└── raw_eloqua_campaigns            (référentiel — appel existant, allégé)

Silver (dédup, typage, enrichissement)
├── slv_eloqua_events_send / open / bounce / click
│     → ROW_NUMBER() sur clé technique, cast des timestamps
└── slv_eloqua_dim_campaign         (id, name, start_date)

Gold (reconstruction à l'identique)
├── eloqua_campaigns_metrics        (schéma inchangé)
└── eloqua_campaigns_liens          (schéma inchangé)
```

### 3.3 Orchestration quotidienne

1. **Extract Bronze** : fenêtre J-1 sur la date d'événement de chaque endpoint (paramétrable pour backfill). Écriture idempotente par partition (`WRITE_TRUNCATE` sur la partition de date d'événement).
2. **Refresh dimension** : appel campaign-level pour les campagnes récentes/modifiées.
3. **Détection des campagnes impactées** : `SELECT DISTINCT eloquaCampaignID` sur les partitions Bronze fraîchement chargées.
4. **Recompute ciblé** : recalcul **complet** des métriques pour ces seules campagnes, depuis Silver (pas d'incrément de compteurs → idempotence garantie).
5. **MERGE Gold** sur `eloqua_campaign_id` (+ `clickthrough_link` pour la table liens).

### 3.4 Avantages

- ✅ Répond directement au besoin : les campagnes anciennes avec événements tardifs sont mises à jour automatiquement.
- ✅ Aucune dépendance à une évolution API supplémentaire → **implémentable immédiatement**.
- ✅ Schémas Gold inchangés → transparent pour les consommateurs aval.
- ✅ Recompute ciblé par campagne → coût BigQuery maîtrisé, pas de full scan historique.
- ✅ Partitionnement par date d'événement (et non `ingestion_date`) → backfills cohérents et rejouables.

### 3.5 Inconvénients / risques

- ⚠️ Deux mécanismes d'extraction coexistent (events + campaign-level) → complexité opérationnelle légèrement supérieure.
- ⚠️ Si une campagne est **renommée** côté Eloqua sans nouvel événement, le nom en Gold n'est rafraîchi qu'au prochain passage du référentiel (fréquence à définir : quotidien full léger recommandé).
- ⚠️ Risque d'écart entre les métriques recalculées et les métriques historiques campaign-level (définitions d'agrégats côté Eloqua potentiellement différentes) → **phase de recette comparative obligatoire** (double run).

---

## 4. Option B — 100 % event-level avec un 6e endpoint « campaign »

### 4.1 Principe

Demander à l'équipe API un **6e endpoint référentiel campagne** (`mailing/campaigns/v1/campaign` par exemple) exposant a minima : `eloquaCampaignID`, `campaignName`, `campaignStartDate`, `lastModifiedAt`. L'appel campaign-level actuel (avec métriques agrégées) est **entièrement décommissionné**.

### 4.2 Architecture

Identique à l'option A, à la différence près :

```
Bronze
└── raw_eloqua_campaign             (6e endpoint, incrémental sur lastModifiedAt)
```

La dimension campagne est extraite avec la même mécanique que les événements (fenêtre incrémentale sur `lastModifiedAt`), ce qui homogénéise totalement le pipeline : **un seul pattern d'extraction, un seul framework de backfill**.

### 4.3 Avantages

- ✅ Architecture homogène : 6 endpoints, un pattern unique d'extraction/backfill → maintenance simplifiée à long terme.
- ✅ Décommissionnement complet de l'ancien flux campaign-level → une seule source de vérité.
- ✅ Renommages de campagnes captés naturellement via `lastModifiedAt`.
- ✅ Ouvre la voie à des cas d'usage futurs (SCD sur les campagnes, historisation des changements de nom/dates).

### 4.4 Inconvénients / risques

- ⚠️ **Dépendance à une évolution API** : délai de mise à disposition inconnu, hors de notre contrôle.
- ⚠️ Périmètre du champ `lastModifiedAt` côté source à valider (couvre-t-il tous les changements pertinents ?).
- ⚠️ Nécessite une phase de transition pendant laquelle l'ancien flux reste actif.

---

## 5. Comparatif synthétique

| Critère | Option A (hybride) | Option B (full event + endpoint campaign) |
|---|---|---|
| Délai de mise en œuvre | ✅ Immédiat | ⚠️ Dépend de l'équipe API |
| Homogénéité du pipeline | ⚠️ Deux patterns d'extraction | ✅ Pattern unique |
| Fraîcheur du référentiel campagne | ⚠️ Selon fréquence du refresh dimension | ✅ Incrémental natif |
| Réponse au besoin (MAJ campagnes anciennes) | ✅ Oui | ✅ Oui |
| Risque projet | ✅ Faible | ⚠️ Dépendance externe |
| Maintenance long terme | ⚠️ Moyenne | ✅ Faible |

---

## 6. Recommandation

**Démarrer en Option A, avec une architecture "Option B-ready".**

L'option A est implémentable dès maintenant et répond au besoin fonctionnel. En structurant le référentiel campagne comme une table Bronze/Silver à part entière (`raw_eloqua_campaigns` → `slv_eloqua_dim_campaign`), la bascule vers l'option B se résume, le jour où le 6e endpoint est disponible, à **remplacer la source d'alimentation de `raw_eloqua_campaigns`** — sans toucher au reste du pipeline ni aux tables Gold.

En parallèle : **formuler dès maintenant la demande du 6e endpoint** auprès de l'équipe API (spécification minimale : id, name, start_date, lastModifiedAt, pagination, fenêtre de filtrage sur lastModifiedAt).

---

## 7. Points à trancher (communs aux deux options)

1. **`clickthrough_link` en BYTES** dans la table actuelle : héritage (hash ?) à clarifier. Proposition : passage en STRING à l'occasion du refactor, sauf contrainte de compatibilité aval.
2. **Sémantique de `firstOpen` / `firstClick`** : premier événement par contact ? par send ? → impacte le calcul de `unique_opens` / `unique_clickthrough_rate`. À valider avec l'équipe API.
3. **Pagination et rate limits** des endpoints : volumétrie attendue par jour pour dimensionner la fenêtre d'extraction et le parallélisme.
4. **Définition exacte des rates** (`open_rate`, `delivered_rate`, etc.) : reproduire les formules actuelles à l'identique ou les redéfinir proprement (dénominateur sends vs delivered).
5. **Stratégie de recette** : double run (ancien flux vs nouveau) sur une période de référence, réconciliation campagne par campagne, documentation des écarts résiduels (événements tardifs justement captés par le nouveau flux).
6. **Backfill initial** : profondeur d'historique disponible sur les endpoints events ? Si limitée, les métriques historiques devront être initialisées depuis l'ancien flux (snapshot de bascule).

---

## 8. Prochaines étapes

- [ ] Validation de la recommandation (Option A, architecture B-ready)
- [ ] Demande formelle du 6e endpoint « campaign » à l'équipe API
- [ ] Réponses aux points 1–3 (BYTES, sémantique first*, volumétrie)
- [ ] Conception détaillée du DAG Airflow (extraction fenêtrée + backfill paramétrable)
- [ ] Spécification SQL des agrégations Gold + plan de recette comparative
