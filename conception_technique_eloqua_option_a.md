# Refactor pipeline Eloqua — Conception technique détaillée (Option A)

**Statut** : Conception technique — à valider
**Architecture retenue** : Raw (event-level, partitionné par date d'événement) → Mart (reconstruction à l'identique)
**Fenêtre d'extraction** : J-30 glissant sur la date d'événement
**Auteur** : Amar SEBAA
**Date** : Juillet 2026

---

## 1. Vue d'ensemble

```
                     ┌─────────────────────────────────────────────┐
   API Eloqua        │  Raw (partitionné par date d'événement)     │
                     │                                             │
 email-send ───────► │  raw_eloqua_email_send                      │
 email-open ───────► │  raw_eloqua_email_open                      │
 email-bounceback ─► │  raw_eloqua_email_bounceback                │       Mart
 email-clickthrough► │  raw_eloqua_email_clickthrough              │  ┌──────────────────────────────┐
 email-asset ──────► │  raw_eloqua_email_asset                     │─►│ eloqua_campaigns_metrics     │
 campaigns (exist.)► │  raw_eloqua_campaigns  (référentiel)        │  │ eloqua_campaigns_liens       │
                     └─────────────────────────────────────────────┘  └──────────────────────────────┘
                        Fenêtre J-30, écriture idempotente               MERGE ciblé sur les campagnes
                        par partition                                    impactées par la fenêtre
```

Principe : les 4 endpoints d'événements alimentent des tables Raw partitionnées par **date d'événement**. À chaque run, on détecte les `eloqua_campaign_id` touchés par la fenêtre J-30, on **recalcule intégralement** leurs métriques depuis Raw, puis on MERGE dans les tables Mart (schémas inchangés). La déduplication est intégrée à l'étape de recompute (CTE dédiées), sans couche physique intermédiaire.

---

## 2. Couche Raw — DDL des tables

Conventions communes :
- Dataset : `dw_echonet_eloqua` (ou dataset `raw_` dédié selon les conventions du datalab — à confirmer)
- Colonnes API converties en `snake_case`
- 2 colonnes techniques ajoutées : `event_date` (colonne de partition, dérivée de la date d'événement) et `ingestion_ts` (horodatage du run)
- Partitionnement : `PARTITION BY event_date`, clustering sur `eloqua_campaign_id`
- Rétention de partition : aucune (historique complet nécessaire au recompute)

### 2.1 `raw_eloqua_email_send`

```sql
CREATE TABLE IF NOT EXISTS `dw_echonet_eloqua.raw_eloqua_email_send` (
  email_send_id             INT64     NOT NULL,   -- emailSendID (clé technique)
  eloqua_campaign_id        INT64,                -- eloquaCampaignID
  account_id                INT64,
  contact_id                INT64,
  eloqua_linked_account_id  INT64,
  email_id                  INT64,
  segment_id                INT64,
  sent_date_hour            TIMESTAMP,            -- sentDateHour
  event_date                DATE      NOT NULL,   -- DATE(sent_date_hour)
  ingestion_ts              TIMESTAMP NOT NULL
)
PARTITION BY event_date
CLUSTER BY eloqua_campaign_id;
```

### 2.2 `raw_eloqua_email_open`

```sql
CREATE TABLE IF NOT EXISTS `dw_echonet_eloqua.raw_eloqua_email_open` (
  email_open_id             INT64     NOT NULL,   -- emailOpenID (clé technique)
  eloqua_campaign_id        INT64,
  account_id                INT64,
  contact_id                INT64,
  eloqua_linked_account_id  INT64,
  email_id                  INT64,
  segment_id                INT64,
  open_date_hour            TIMESTAMP,            -- openDateHour
  sent_date_hour            TIMESTAMP,
  device_info_id            INT64,                -- deviceInfoId
  first_open                INT64,                -- firstOpen (0/1 — sémantique à confirmer)
  event_date                DATE      NOT NULL,   -- DATE(open_date_hour)
  ingestion_ts              TIMESTAMP NOT NULL
)
PARTITION BY event_date
CLUSTER BY eloqua_campaign_id;
```

### 2.3 `raw_eloqua_email_bounceback`

```sql
CREATE TABLE IF NOT EXISTS `dw_echonet_eloqua.raw_eloqua_email_bounceback` (
  email_bounceback_id       INT64     NOT NULL,   -- emailBouncebackID (clé technique)
  eloqua_campaign_id        INT64,
  account_id                INT64,
  contact_id                INT64,
  eloqua_linked_account_id  INT64,
  email_id                  INT64,
  segment_id                INT64,
  bounce_date_hour          TIMESTAMP,            -- bounceDateHour
  sent_date_hour            TIMESTAMP,
  bounce_type               STRING,               -- bounceType (hard/soft — valeurs à confirmer)
  event_date                DATE      NOT NULL,   -- DATE(bounce_date_hour)
  ingestion_ts              TIMESTAMP NOT NULL
)
PARTITION BY event_date
CLUSTER BY eloqua_campaign_id;
```

### 2.4 `raw_eloqua_email_clickthrough`

```sql
CREATE TABLE IF NOT EXISTS `dw_echonet_eloqua.raw_eloqua_email_clickthrough` (
  email_clickthrough_id           INT64     NOT NULL,   -- emailClickthroughId (clé technique)
  eloqua_campaign_id              INT64,
  account_id                      INT64,
  contact_id                      INT64,
  email_id                        INT64,
  segment_id                      INT64,
  sent_date_hour                  TIMESTAMP,
  base_url_id                     INT64,                -- baseUrlId
  click_date_hour                 TIMESTAMP,            -- clickDateHour
  first_click                     INT64,                -- firstClick (0/1 — sémantique à confirmer)
  clickthrough_link               STRING,               -- clickThroughLink (STRING natif côté API)
  clickthrough_query_string_value STRING,               -- clickthroughQueryStringValue
  event_date                      DATE      NOT NULL,   -- DATE(click_date_hour)
  ingestion_ts                    TIMESTAMP NOT NULL
)
PARTITION BY event_date
CLUSTER BY eloqua_campaign_id;
```

> Note : l'API renvoie `clickThroughLink` en STRING. La conversion vers le type `BYTES` de la table Mart actuelle (héritage) est faite uniquement au moment du MERGE Mart (voir §5.3) pour préserver la compatibilité aval.

### 2.5 `raw_eloqua_email_asset` (référentiel email)

Pas un flux d'événements → pas de partition par date d'événement. Chargement full à chaque run (volumétrie faible attendue) ou incrémental sur `last_modified_at`, avec MERGE sur `email_asset_id`.

```sql
CREATE TABLE IF NOT EXISTS `dw_echonet_eloqua.raw_eloqua_email_asset` (
  email_asset_id     INT64     NOT NULL,   -- emailAssetID (clé)
  email_id           INT64,
  asset_name         STRING,
  asset_type         STRING,
  created_by         INT64,
  created_at         TIMESTAMP,
  last_modified_by   INT64,
  last_modified_at   TIMESTAMP,
  email_group_id     INT64,                -- emailGroupID
  subject_line       STRING,
  email_category     STRING,
  ingestion_ts       TIMESTAMP NOT NULL
)
CLUSTER BY email_id;
```

### 2.6 `raw_eloqua_campaigns` (référentiel campagne — dimension)

Alimentée par l'appel campaign-level **existant**, rétrogradé au rôle de dimension : seules les colonnes descriptives sont conservées, les métriques agrégées ne sont plus utilisées. Table **cumulative** (MERGE, jamais tronquée).

```sql
CREATE TABLE IF NOT EXISTS `dw_echonet_eloqua.raw_eloqua_campaigns` (
  eloqua_campaign_id   INT64     NOT NULL,   -- clé
  campaign_name        STRING,
  campaign_start_date  TIMESTAMP,
  first_seen_ts        TIMESTAMP NOT NULL,   -- premier chargement dans la dim
  ingestion_ts         TIMESTAMP NOT NULL    -- dernier refresh
)
CLUSTER BY eloqua_campaign_id;
```

MERGE d'alimentation (à chaque run, sur la fenêtre M-1 de l'appel existant) :

```sql
MERGE `dw_echonet_eloqua.raw_eloqua_campaigns` T
USING staging_campaigns S
ON T.eloqua_campaign_id = S.eloqua_campaign_id
WHEN MATCHED THEN UPDATE SET
  campaign_name       = S.campaign_name,
  campaign_start_date = S.campaign_start_date,
  ingestion_ts        = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT
  (eloqua_campaign_id, campaign_name, campaign_start_date, first_seen_ts, ingestion_ts)
VALUES
  (S.eloqua_campaign_id, S.campaign_name, S.campaign_start_date,
   CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
```

> **Backfill initial obligatoire** : un one-shot chargeant toutes les campagnes historiques dans la dim avant la mise en service (sinon les events tardifs de campagnes anciennes seraient orphelins).

---

## 3. Couche Mart — schémas cibles (inchangés)

### 3.1 `eloqua_campaigns_metrics`

| Colonne | Type | Règle de calcul |
|---|---|---|
| `eloqua_campaign_id` | INTEGER | Clé de MERGE |
| `campaign_name` | STRING | Depuis `raw_eloqua_campaigns` |
| `campaign_start_date` | TIMESTAMP | Depuis `raw_eloqua_campaigns` |
| `total_sends` | INTEGER | `COUNT(DISTINCT email_send_id)` |
| `total_opens` | INTEGER | `COUNT(DISTINCT email_open_id)` |
| `unique_opens` | INTEGER | `COUNT(DISTINCT contact_id)` sur opens ¹ |
| `total_delivered` | INTEGER | `total_sends − total_bounces` |
| `total_clickthroughs` | INTEGER | `COUNT(DISTINCT email_clickthrough_id)` |
| `delivered_rate` | FLOAT | `total_delivered / NULLIF(total_sends, 0)` |
| `open_rate` | FLOAT | `total_opens / NULLIF(total_delivered, 0)` ² |
| `unique_open_rate` | FLOAT | `unique_opens / NULLIF(total_delivered, 0)` ² |
| `clickthrough_rate` | FLOAT | `total_clickthroughs / NULLIF(total_delivered, 0)` ² |
| `unique_clickthrough_rate` | FLOAT | `unique_clicks / NULLIF(total_delivered, 0)` ² |
| `is_clickthroughed` | INTEGER | `IF(total_clickthroughs > 0, 1, 0)` |
| `ingestion_date` | DATE | Date du run |

¹ Alternative : `SUM(first_open)` si `firstOpen` = « premier open du contact pour ce send » — **à trancher en recette** (comparaison des deux définitions vs valeurs historiques).
² Dénominateur `total_delivered` vs `total_sends` : **à valider en reproduisant les valeurs actuelles** sur un échantillon de campagnes. Les formules seront figées à l'issue de la recette.

### 3.2 `eloqua_campaigns_liens`

| Colonne | Type | Règle de calcul |
|---|---|---|
| `eloqua_campaign_id` | INTEGER | Clé de MERGE (composante 1) |
| `clickthrough_link` | BYTES | Clé de MERGE (composante 2) — `CAST(clickthrough_link AS BYTES)` ³ |
| `campaign_name` | STRING | Depuis `raw_eloqua_campaigns` |
| `campaign_start_date` | TIMESTAMP | Depuis `raw_eloqua_campaigns` |
| `total_clickthroughs` | INTEGER | `COUNT(DISTINCT email_clickthrough_id)` par lien |
| `ingestion_date` | DATE | Date du run |

³ À confirmer : si le BYTES actuel est un simple encodage UTF-8 du lien, `CAST(link AS BYTES)` suffit ; si c'est un hash (SHA/MD5), reproduire la fonction de hash existante. Vérification à faire sur un échantillon (`SAFE_CONVERT_BYTES_TO_STRING(clickthrough_link)` sur la table actuelle).

---

## 4. Orchestration quotidienne (Airflow)

DAG `etl_eloqua_events` — schedule quotidien. Fenêtre par défaut : `[data_interval_end − 30j ; data_interval_end[` sur la date d'événement, surchargeable par params pour backfill (pattern `create_dag` + trigger REST API, comme sur `etl_eloqua` existant).

```
[extract_send] ──┐
[extract_open] ──┤
[extract_bounce]─┼──► [refresh_dim_campaigns] ──► [check_orphan_campaigns] ──► [detect_impacted] ──► [recompute_metrics] ──► [merge_metrics]
[extract_click]──┤                                                                              └──► [recompute_liens]  ──► [merge_liens]
[extract_asset]──┘
```

| Tâche | Rôle | Idempotence |
|---|---|---|
| `extract_*` (×4 events) | Appel API sur la fenêtre J-30, écriture des partitions `event_date` concernées | `WRITE_TRUNCATE` **par partition** (decorator `$YYYYMMDD` ou `MERGE` par plage) — rejouable sans doublons |
| `extract_asset` | Full ou incrémental `lastModifiedAt`, MERGE sur `email_asset_id` | MERGE |
| `refresh_dim_campaigns` | Appel campaign-level existant (fenêtre M-1), MERGE dans la dim | MERGE |
| `check_orphan_campaigns` | Contrôle : events de la fenêtre sans entrée dans la dim → alerte (+ appel ciblé par ID si l'API le permet) | Lecture seule |
| `detect_impacted` | `SELECT DISTINCT eloqua_campaign_id` sur les partitions de la fenêtre (4 tables events, UNION) → table temporaire `_impacted_campaigns` du run | Recréée à chaque run |
| `recompute_*` | Recalcul **complet** des métriques pour les seules campagnes impactées (staging du run) | Recréée à chaque run |
| `merge_*` | MERGE dans les tables Mart | MERGE sur clé |

**Pourquoi J-30 et pas J-1** : la fenêtre de 30 jours absorbe les events livrés en retard par l'API et les trous d'exploitation (runs manqués) sans mécanique de rattrapage dédiée. Coût : re-scan de 30 partitions par table event et par jour — acceptable en volumétrie, et le recompute reste borné aux campagnes réellement touchées. La fenêtre est un paramètre du DAG (`lookback_days`, défaut 30).

---

## 5. SQL détaillé

### 5.1 Détection des campagnes impactées

```sql
CREATE OR REPLACE TABLE `dw_echonet_eloqua._impacted_campaigns_{{ run_id }}` AS
SELECT DISTINCT eloqua_campaign_id
FROM (
  SELECT eloqua_campaign_id FROM `dw_echonet_eloqua.raw_eloqua_email_send`
   WHERE event_date BETWEEN @window_start AND @window_end
  UNION ALL
  SELECT eloqua_campaign_id FROM `dw_echonet_eloqua.raw_eloqua_email_open`
   WHERE event_date BETWEEN @window_start AND @window_end
  UNION ALL
  SELECT eloqua_campaign_id FROM `dw_echonet_eloqua.raw_eloqua_email_bounceback`
   WHERE event_date BETWEEN @window_start AND @window_end
  UNION ALL
  SELECT eloqua_campaign_id FROM `dw_echonet_eloqua.raw_eloqua_email_clickthrough`
   WHERE event_date BETWEEN @window_start AND @window_end
)
WHERE eloqua_campaign_id IS NOT NULL;
```

### 5.2 Recompute `eloqua_campaigns_metrics` (staging)

La dédup (`COUNT(DISTINCT clé_technique)`) neutralise les doublons potentiels entre fenêtres qui se chevauchent — c'est ce qui permet de se passer d'une couche physique intermédiaire.

```sql
CREATE OR REPLACE TABLE `dw_echonet_eloqua._stg_metrics_{{ run_id }}` AS
WITH sends AS (
  SELECT eloqua_campaign_id,
         COUNT(DISTINCT email_send_id) AS total_sends
  FROM `dw_echonet_eloqua.raw_eloqua_email_send`
  WHERE eloqua_campaign_id IN (SELECT eloqua_campaign_id FROM `_impacted_campaigns_{{ run_id }}`)
  GROUP BY 1
),
opens AS (
  SELECT eloqua_campaign_id,
         COUNT(DISTINCT email_open_id) AS total_opens,
         COUNT(DISTINCT contact_id)    AS unique_opens
  FROM `dw_echonet_eloqua.raw_eloqua_email_open`
  WHERE eloqua_campaign_id IN (SELECT eloqua_campaign_id FROM `_impacted_campaigns_{{ run_id }}`)
  GROUP BY 1
),
bounces AS (
  SELECT eloqua_campaign_id,
         COUNT(DISTINCT email_bounceback_id) AS total_bounces
  FROM `dw_echonet_eloqua.raw_eloqua_email_bounceback`
  WHERE eloqua_campaign_id IN (SELECT eloqua_campaign_id FROM `_impacted_campaigns_{{ run_id }}`)
  GROUP BY 1
),
clicks AS (
  SELECT eloqua_campaign_id,
         COUNT(DISTINCT email_clickthrough_id) AS total_clickthroughs,
         COUNT(DISTINCT contact_id)            AS unique_clicks
  FROM `dw_echonet_eloqua.raw_eloqua_email_clickthrough`
  WHERE eloqua_campaign_id IN (SELECT eloqua_campaign_id FROM `_impacted_campaigns_{{ run_id }}`)
  GROUP BY 1
)
SELECT
  c.eloqua_campaign_id,
  d.campaign_name,
  d.campaign_start_date,
  IFNULL(s.total_sends, 0)                                        AS total_sends,
  IFNULL(o.total_opens, 0)                                        AS total_opens,
  IFNULL(o.unique_opens, 0)                                       AS unique_opens,
  IFNULL(s.total_sends, 0) - IFNULL(b.total_bounces, 0)           AS total_delivered,
  IFNULL(k.total_clickthroughs, 0)                                AS total_clickthroughs,
  SAFE_DIVIDE(IFNULL(s.total_sends,0) - IFNULL(b.total_bounces,0),
              NULLIF(s.total_sends, 0))                           AS delivered_rate,
  SAFE_DIVIDE(o.total_opens,
              NULLIF(IFNULL(s.total_sends,0) - IFNULL(b.total_bounces,0), 0)) AS open_rate,
  SAFE_DIVIDE(o.unique_opens,
              NULLIF(IFNULL(s.total_sends,0) - IFNULL(b.total_bounces,0), 0)) AS unique_open_rate,
  SAFE_DIVIDE(k.total_clickthroughs,
              NULLIF(IFNULL(s.total_sends,0) - IFNULL(b.total_bounces,0), 0)) AS clickthrough_rate,
  SAFE_DIVIDE(k.unique_clicks,
              NULLIF(IFNULL(s.total_sends,0) - IFNULL(b.total_bounces,0), 0)) AS unique_clickthrough_rate,
  IF(IFNULL(k.total_clickthroughs, 0) > 0, 1, 0)                  AS is_clickthroughed,
  CURRENT_DATE()                                                  AS ingestion_date
FROM `_impacted_campaigns_{{ run_id }}` c
LEFT JOIN `dw_echonet_eloqua.raw_eloqua_campaigns` d USING (eloqua_campaign_id)
LEFT JOIN sends   s USING (eloqua_campaign_id)
LEFT JOIN opens   o USING (eloqua_campaign_id)
LEFT JOIN bounces b USING (eloqua_campaign_id)
LEFT JOIN clicks  k USING (eloqua_campaign_id);
```

### 5.3 MERGE Mart

```sql
-- metrics
MERGE `dw_echonet_eloqua.eloqua_campaigns_metrics` T
USING `dw_echonet_eloqua._stg_metrics_{{ run_id }}` S
ON T.eloqua_campaign_id = S.eloqua_campaign_id
WHEN MATCHED THEN UPDATE SET
  campaign_name = S.campaign_name,  campaign_start_date = S.campaign_start_date,
  total_sends = S.total_sends,      total_opens = S.total_opens,
  unique_opens = S.unique_opens,    total_delivered = S.total_delivered,
  total_clickthroughs = S.total_clickthroughs,
  delivered_rate = S.delivered_rate, open_rate = S.open_rate,
  unique_open_rate = S.unique_open_rate, clickthrough_rate = S.clickthrough_rate,
  unique_clickthrough_rate = S.unique_clickthrough_rate,
  is_clickthroughed = S.is_clickthroughed, ingestion_date = S.ingestion_date
WHEN NOT MATCHED THEN INSERT ROW;

-- liens (staging analogue à 5.2, agrégé par campagne + lien)
MERGE `dw_echonet_eloqua.eloqua_campaigns_liens` T
USING (
  SELECT S.*, CAST(S.clickthrough_link_str AS BYTES) AS clickthrough_link
  FROM `dw_echonet_eloqua._stg_liens_{{ run_id }}` S
) S
ON  T.eloqua_campaign_id = S.eloqua_campaign_id
AND T.clickthrough_link  = S.clickthrough_link
WHEN MATCHED THEN UPDATE SET
  campaign_name = S.campaign_name, campaign_start_date = S.campaign_start_date,
  total_clickthroughs = S.total_clickthroughs, ingestion_date = S.ingestion_date
WHEN NOT MATCHED THEN INSERT
  (eloqua_campaign_id, clickthrough_link, campaign_name, campaign_start_date,
   total_clickthroughs, ingestion_date)
VALUES
  (S.eloqua_campaign_id, S.clickthrough_link, S.campaign_name, S.campaign_start_date,
   S.total_clickthroughs, S.ingestion_date);
```

> ⚠️ Pour la table `liens`, le MERGE ne supprime pas les lignes de liens qui disparaîtraient d'un recompute (cas théorique : correction de données côté source). Si ce cas doit être couvert : ajouter `WHEN NOT MATCHED BY SOURCE AND T.eloqua_campaign_id IN (SELECT ... impacted) THEN DELETE`.

---

## 6. Backfill

Deux niveaux, même mécanique que le quotidien :

1. **Backfill Raw** : rejouer `extract_*` sur une plage `[start_date ; end_date]` de dates d'événement (params du DAG via trigger REST, pattern existant `etl_eloqua`). Idempotent par construction (truncate par partition).
2. **Backfill Mart** : la détection des campagnes impactées porte alors sur la plage rejouée → recompute + MERGE des campagnes concernées. Aucune logique spécifique.

**Backfill initial de mise en service** :
- Charger l'historique events aussi loin que l'API le permet (profondeur à confirmer — point ouvert §8.3)
- One-shot dim campagnes : toutes les campagnes historiques
- Pour les campagnes dont l'historique events est incomplet (antérieur à la profondeur API) : **conserver les lignes Mart actuelles** (le MERGE ne touche que les campagnes impactées — les anciennes valeurs campaign-level restent en place comme snapshot de bascule)

---

## 7. Contrôles qualité (intégrés au DAG)

| Contrôle | Tâche | Seuil / action |
|---|---|---|
| Events orphelins (campagne absente de la dim) | `check_orphan_campaigns` | > 0 → alerte (WARN, non bloquant) ; name/start_date à NULL en attendant |
| Doublons de clé technique intra-partition | post-extract | > 0 → alerte (l'agrégat DISTINCT neutralise, mais signale un problème API) |
| Volumétrie extract vs J-7 même jour de semaine | post-extract | Écart > ±50 % → WARN |
| `total_delivered < 0` (bounces > sends) | post-recompute | > 0 lignes → FAIL (incohérence de fenêtre) |
| Non-régression rates | recette uniquement | Double run vs flux actuel, écarts documentés |

---

## 8. Points ouverts (bloquants avant dev)

1. **Sémantique `first_open` / `first_click`** (par contact ? par send ?) → conditionne la formule des uniques (§3.1 note ¹). Action : question à l'équipe API + test comparatif en recette.
2. **Nature du BYTES `clickthrough_link`** (encodage direct vs hash) → conditionne le CAST du §5.3. Action : `SAFE_CONVERT_BYTES_TO_STRING` sur un échantillon de la table actuelle.
3. **Profondeur d'historique des endpoints events** → conditionne le backfill initial (§6). Action : question à l'équipe API.
4. **Dénominateur des rates** (sends vs delivered) → reproduction à l'identique des valeurs actuelles sur échantillon avant de figer les formules (§3.1 note ²).
5. **Pagination / rate limits des endpoints** → dimensionnement du parallélisme des `extract_*` et de la fenêtre max en backfill.
6. **Dataset cible des tables Raw** (`dw_echonet_eloqua` vs dataset raw dédié) → conventions datalab.

---

## 9. Plan de recette

1. **Double run** sur 4 semaines : ancien flux (campaign-level) et nouveau flux (events) en parallèle, tables Mart nouvelles écrites dans un dataset `_recette`.
2. **Réconciliation campagne par campagne** : écarts sur chaque métrique, classés en (a) écarts de définition (uniques, dénominateurs) → ajustement des formules, (b) écarts justifiés par les events tardifs (le nouveau flux est plus juste) → documentés, (c) écarts inexpliqués → investigation.
3. **Validation du CAST liens** : jointure `liens_actuel` vs `liens_recette` sur (campagne, lien), taux de correspondance attendu ~100 % hors events tardifs.
4. **Bascule** : gel de l'ancien flux, snapshot des tables Mart (sauvegarde), activation du nouveau DAG, période d'observation 2 semaines avec l'ancien DAG désactivé mais réactivable.
