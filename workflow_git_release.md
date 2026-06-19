# Proposition de workflow Git : introduction d'une branche `release`

**Auteur :** Équipe Data Engineering · **Statut :** Proposition à valider

---

## Problématique

Aujourd'hui :

- Une seule branche principale : `master`.
- Les features sont mergées **directement dans `master`**, et la MEP part de `master`.
- **Aucune CI** : rien ne valide le code avant qu'il n'arrive en prod.

Conséquences :

- `master` n'est jamais un état « garanti sain » : du code non validé part en prod.
- La MEP, portée par un profil peu technique, est le **seul** moment où les erreurs apparaissent — au pire moment.

## Solution proposée

Introduire une branche **`release`** comme sas d'intégration et de validation entre les features et `master`. `master` devient une branche de **production stricte** : uniquement du code validé.

```
feature/xxx ─┐
feature/yyy ─┼──►  release  ──(validation OK, jour de MEP)──►  master ──► PROD
feature/zzz ─┘
```

**Étapes :**

1. Les features sont créées à partir de `release`.
2. Quand une feature est prête, elle est mergée dans `release` (via MR relue).
3. On valide que `release` est saine (voir CI ci-dessous).
4. **Seulement si `release` est verte**, le jour de la MEP, **un dev** merge `release` → `master`.
5. La MEP part de `master`, état stable et validé.

> Règle d'or : on ne merge dans `master` que depuis une `release` validée, le jour de la MEP.

## Mise en place d'une CI légère

Comme nous n'avons aucune CI, on propose un premier niveau **simple et rapide**, **sans connexion externe** (pas d'accès GCP, pas de secrets, pas de déploiement). Objectif : bloquer les erreurs évidentes avant le merge.

Sur chaque MR vers `release` :

- **Lint Python** : `ruff` (ou `flake8`) + `black --check` pour le style, et `python -m py_compile` pour garantir qu'il n'y a pas d'erreur de syntaxe.
- **Lint YAML** : `yamllint` sur les fichiers de configuration.

Ces vérifications tournent en quelques secondes, en local ou sur un runner sans accès réseau, et constituent le critère « `release` est verte » de l'étape 3.

## Pourquoi c'est intéressant

- `master` reste toujours déployable → moins d'erreurs de MEP.
- Les erreurs sont détectées en amont (à froid), pas pendant le déploiement (à chaud).
- La MEP devient un geste simple et sûr, moins dépendant du niveau technique de celui qui déploie.
- Premier socle de qualité (lint Python + YAML) sans aucune dépendance externe.

## Points d'attention

- Les features doivent partir de `release` pour éviter les divergences.
- Après une MEP, réaligner `release` et `master`.
- Prévoir un cas **hotfix** (branche depuis `master`, puis report sur `release`).

## Conclusion

L'ajout de `release` crée un sas de validation léger qui **garantit un `master` toujours sain**, et la CI légère (lint Python + YAML, sans accès externe) pose un premier filet de sécurité immédiat et peu coûteux.

**Prochaine étape :** valider le workflow en équipe et choisir les outils de lint (ruff/flake8, yamllint).
