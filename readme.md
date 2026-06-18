# Onboarding — Périmètre M7 (global-core-model-customer)

Doc d'onboarding pour prendre en main rapidement le périmètre **M7** du dépôt Canal+
`global-core-model-customer-dags`. Basé sur l'analyse de **tous les fichiers contenant `m7`**.

---

## 1. Contexte en une page

**Le produit `global-core-model`** harmonise les données **clients / contrats / produits**
de plusieurs filiales Canal+ (« territoires ») dans un **modèle cible commun** sur Snowflake.
Chaque territoire arrive avec ses codes locaux ; le projet les transcode en identifiants
globaux et empile tout dans des tables finales unifiées (`CUSTOMER`, `CONTRACT`, etc.).

- **Orchestration** : package Airflow `global_core_model_customer_dags` (framework interne
  `federer-airflow`), tournant sur **MWAA**. Les DAGs ne font que lancer dbt par paquets
  via l'opérateur `DbtTaskGroupSnowflake` (Cosmos).
- **Transformation** : projet **dbt** `global_core_model_customer` sur **Snowflake**.
- **Territoires** présents : `france`, `bios`, **`m7`**, `poland`, `mauritius`, `gva`, `mcg`
  (+ variantes `safe` pour les données personnelles).

**M7** = filiale **M7 Group** (pay-TV européenne du groupe Canal+). Dans le dépôt, son point
d'entrée est la source dbt **`customer_m7`** (+ `event_m7`). **« BCE »** n'est pas un objet du
dépôt : c'est le **système source amont** (gestion abonnés/facturation côté M7) qui alimente
les tables `interface__*__m7`. Donc *fonctionnellement* : BCE → interfaces M7 → modèle global.

---

## 2. Le flux de données M7 (vue fonctionnelle)

```
[BCE / système source M7]
        │   (livraison des tables d'interface dans <env>_WORK.gl_w_customer)
        ▼
SOURCES dbt : customer_m7 (interface__customer__m7, interface__contract__m7, …)
              event_m7    (interface__event__m7)
        │   1) tests de qualité sur les sources
        ▼
SNAPSHOTS (SCD2)  : snapshot__customer__m7, snapshot__contract__m7,
                    snapshot__contract_commercial_product__m7
        │   historisation des versions (valid_from / valid_to)
        ▼
STAGING           : stg__contract__m7, stg__contract_commercial_products__m7
        │   enrichissement (rattachement client courant, offres, business unit)
        ▼
MARTS (tables finales communes) : customer, contract,
        contract_commercial_products, event, customer_access_rights
        │   UNION de tous les territoires + Row Access Policy par pays
        ▼
PORTFOLIO (agrégats) : portfolio_agg
```

Le « cœur métier » du modèle, c'est la couche **snapshots → staging → marts**. M7 ne réécrit
jamais cette logique : il **réutilise les modèles/macros communs**, paramétrés pour M7.

---

## 3. Inventaire des fichiers M7 (ce qui contient `m7`)

| Fichier | Rôle |
|---|---|
| `..._run_transformations_m7.py` | **DAG dédié M7** (déclenché à la demande) |
| `run_customer_transformations_dag.py` | **DAG maître** multi-pays (construit M7 *avec* les autres) |
| `_src_customer.yml` (source `customer_m7`) | déclaration des interfaces client/contrat/produit M7 |
| `_src_event.yml` (source `event_m7`) | déclaration de `interface__event__m7` |
| `stg__contract__m7.sql` | staging contrat M7 (appelle la macro `contract_enrich`) |
| `stg__contract_commercial_products__m7.sql` | staging produits commerciaux du contrat M7 |
| `_customer_snapshot_models.yml` | config SCD2 `snapshot__customer__m7` (+ comm. preferences) |
| `_contract_snapshot_models.yml` | config SCD2 `snapshot__contract__m7` |
| `_product_snapshot_models.yml` | config SCD2 snapshot produit M7 |
| `_customer__models.yml` | doc/tests des marts (où M7 est unionné) |
| `customer.sql`, `contract.sql`, `contract_commercial_products.sql`, `event.sql`, `customer_access_rights.sql` | **marts** : union des territoires (M7 inclus) |
| `_transco_config.yml` | référentiels de transcodage (codes locaux M7 → globaux) |
| `seed__contract_wholesale_partner.csv` | seed de mapping (contient des lignes M7) |
| `contract_enrich.sql` *(ne contient pas « m7 » mais est appelé par M7)* | **macro centrale** d'enrichissement contrat |

---

## 4. Les deux DAGs (orchestration)

### a) DAG dédié M7 — `..._daily_run_transformations__m7`
- `schedule_interval=None` → **déclenché** (manuel ou par un orchestrateur amont quand BCE a livré).
- Construit **uniquement M7** : tous les `DbtTaskGroupSnowflake` ciblent `tag:m7` /
  `dbt_vars={"country":"m7"}`.
- Enchaînement :
  `test_sources(customer_m7, event_m7)` → `alerting` → `load_transco` →
  `stagings_snapshots` (customer → staging → contract → product → product snapshot) →
  `data_marts` (marts + portfolio_agg).

### b) DAG maître — `..._run_transformations_daily`
- Construit **tous les pays** d'un coup (pas de `country` → marts en mode `all`).
- Ordre des tests : france → bios → poland → m7 → mauritius.
- M7 s'y insère via les mêmes `tag:m7` ; rien de spécifique au-delà de la garde REC.

> En lecture : **un DAG = une suite de `dbt run/test --select <sélecteur>`**. Comprendre un
> DAG = lire ses sélecteurs et leur ordre (`>>`).

---

## 5. Détail technique par couche

### 5.1 Sources (`customer_m7`, `event_m7`)
- Schéma `<env>_WORK.gl_w_customer`, tag `m7`.
- Tables : `interface__customer__m7`, `interface__contract__m7`,
  `interface__contract_commercial_products__m7`, `interface__contract_offers__m7`,
  `interface__offers__m7`, `interface__customer_eligibility__m7`,
  `stg__contract*__m7`, `interface__event__m7`.
- **Désactivées pour M7** (`enabled: false`) : `interface__customer_communication_preferences__m7`,
  `interface__commercial_products__m7`.
- Tests hérités via ancres YAML (`<<: *customer`, `<<: *contract`…) : `not_null`, `unique`,
  `relationships` vers les référentiels, et surtout `expression_is_true: "!= -1"` sur les
  colonnes `*_ID` → vérifie que **tout code local M7 a bien été transcodé** (sinon `-1`).

### 5.2 Snapshots (SCD2) — ex. `snapshot__customer__m7`
Config (partagée par tous les pays via l'ancre `&config_customer`) :
- `strategy: check` avec `check_cols` = liste de colonnes métier (crm_id, country, status,
  type, flags…). Une nouvelle version est créée **uniquement si l'une de ces colonnes change**.
- `unique_key: [customer_id]`.
- Bornes de validité **renommées** : `dbt_valid_from → customer_effective_begin_timestamp`,
  `dbt_valid_to → customer_effective_end_timestamp`, `dbt_scd_id → customer_version_id`.
- `dbt_valid_to_current: to_date('8899-12-31')` → **la version courante porte la date de fin
  `8899-12-31`** (convention à connaître pour toute jointure « version active »).
- `hard_deletes: invalidate` (une suppression source ferme la version au lieu de la perdre).
- `post_hook` : `update_snapshot_unhistorised_columns`, `snap_consolidate_set_valid_from/to_date`,
  `snap_consolidate_delete_double_run('customer_version_id')` → nettoyage des dates et
  **dédoublonnage si le snapshot tourne deux fois le même jour**.

### 5.3 Staging — `stg__contract__m7.sql`
Ne contient qu'un appel de macro :
```sql
{{ contract_enrich(
    source_name='customer_m7',
    source_table_name='interface__contract__m7',
    customer_snp_name='snapshot__customer__m7',
    flg_portfolio_rule="iff(CONTRACT_FREE_FLAG = 0 and CONTRACT_TEST_PROFILE_FLAG = 0,1,0)",
    contract_offers_table_name='interface__contract_offers__m7',
    except_cols=["CONTRACT_ACTIVE_PORTFOLIO_FLAG"]
) }}
```
**La macro `contract_enrich`** (fichier `macros/contract_enrich.sql`) fait, pour n'importe quel pays :
1. CTE `CUSTOMER` : prend la **version courante** du snapshot client (`= '8899-12-31'`),
   joint `seed__organizational_area` puis `seed__business_unit` → récupère `BUSINESS_UNIT_LABEL`
   et `customer_version_id`.
2. CTE `CONTRACT_OFFERS` : associe `CONTRACT_ID → OFFER_ID` depuis la table d'offres.
3. `SELECT` final : toutes les colonnes du contrat source (`dbt_utils.star`, moins `except_cols`)
   + `customer_version_id` + `CONTRACT_OFFER_ID` + un flag portefeuille calculé via la règle
   passée en paramètre (`flg_portfolio_rule`).

→ **C'est LE fichier à lire pour comprendre le staging contrat.** M7 n'en est qu'une instance.

`stg__contract_commercial_products__m7.sql` : joint les produits commerciaux à la **version
courante** du snapshot contrat (`= '8899-12-31'`) pour récupérer `CUSTOMER_COUNTRY_ID` et
`CONTRACT_VERSION_ID`.

### 5.4 Marts — `customer.sql`, `contract.sql`, …
Toutes les marts suivent **le même patron** (lire `customer.sql` une fois suffit) :
- `materialized='incremental'`, `incremental_strategy='delete+insert'`, `unique_key='crm_id'`,
  `cluster_by` sur (id, effective_begin, effective_end).
- **`post_hook`** : `apply_rap_on_create_or_full_refresh('ROW_ACCESS_POLICY_CRM_COUNTRY_ROLE',
  ['CRM_ID','CUSTOMER_COUNTRY_ID'])` → **sécurité au niveau ligne par pays** (chaque rôle ne voit
  que son périmètre).
- **Deux modes** selon le paramètre `country` :
  - **incrémental ciblé** (`country=m7`, via le DAG M7) → ne reconstruit que M7 à partir de
    `snapshot__customer__m7` (macro `select_with_overrides`).
  - **full / all** (DAG maître) → `dbt_utils.union_relations(...)` de **tous les snapshots
    territoire** empilés dans une seule table.
- **Garde REC** : `m7/gva/mcg` (et selon la mart, `poland`) sont **exclus de l'union en
  environnement `rec`** (`if target.name == 'rec'`). Normal de ne pas voir M7 en recette.

### 5.5 Transcos & seeds
`load_transco` charge les tables de correspondance (codes locaux → globaux), matérialisées en
`incremental append` dans `DWH.REF`. Les seeds (`seed__business_unit`, `seed__organizational_area`,
`seed__contract_wholesale_partner`…) fournissent les référentiels utilisés par la macro
d'enrichissement et les marts. Les **seeds M7** ont été ajoutés/mis à jour récemment
(cf. `stage/updating-M7-seeds` du CHANGELOG).

### 5.6 Alerting
Chaque DAG termine par un `PythonOperator` (`utils/model_alerting.email`) qui lit
`sql/get_test_results.sql` et envoie le bilan des tests dbt (avec `territory="m7"` pour le DAG M7).
`on_failure_callback` → PagerDuty groupe `data.ccm`.

---

## 6. Spécificités / pièges M7 à retenir

1. **Date de version courante = `8899-12-31`** : toute jointure « état actuel » filtre là-dessus.
2. **`!= -1` sur les `*_ID`** : un `-1` = code local M7 non transcodé → tester d'abord le
   référentiel/transco avant de suspecter la source.
3. **Garde REC** (`m7_tag_if_rec`, `m7_missing_in_rec`) : en `rec`, les sources `interface__contract__m7`
   et `interface__customer__m7` sont considérées absentes et M7 est exclu des snapshots/stagings/marts.
   → en recette, l'absence de données M7 est **attendue**, pas un bug.
4. **Tables M7 désactivées** : `commercial_products` et `communication_preferences` (`enabled:false`).
5. **DAG M7 non planifié** (`schedule_interval=None`) : il tourne sur déclenchement (livraison BCE).
6. **Sécurité par ligne** (Row Access Policy CRM/pays) appliquée en post-hook des marts.

---

## 7. Pour aller vite : ordre de lecture conseillé
1. `run_transformations_m7.py` — comprendre l'enchaînement (10 min).
2. `contract_enrich.sql` — la vraie logique métier du staging contrat (le fichier clé).
3. `stg__contract__m7.sql` + `stg__contract_commercial_products__m7.sql` — l'instanciation M7.
4. `_customer_snapshot_models.yml` — la mécanique SCD2 (clés, dates de validité).
5. `customer.sql` (mart) — comment M7 rejoint le modèle global + sécurité par pays.
6. `_src_customer.yml` / `_src_event.yml` — la liste exacte des tables d'interface M7 et leurs tests.

## 8. Fichiers fournis dans `code/`
DAG M7 + DAG maître, macro `contract_enrich`, stagings M7, marts `customer`/`contract`.
Les snapshots/sources/transcos complets restent sur le Drive (non mirrorés ici).
