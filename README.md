# Adventure Works

Ce projet est un utilitaire permettant de télécharger, traiter et extraire des données depuis des fichiers au format Parquet stockés dans un conteneur Azure Blob Storage. Les images et métadonnées extraites sont organisées et sauvegardées localement pour faciliter leur utilisation dans des projets d’analyse ou d’apprentissage machine.

## Fonctionnalités
* Téléchargement automatique des fichiers Parquet depuis un conteneur Azure Blob Storage.
* Extraction des images et métadonnées contenues dans les fichiers Parquet.
* Organisation locale des fichiers (images dans des sous-dossiers, métadonnées exportées au format CSV).
* Nettoyage automatique des fichiers temporaires et des dossiers vides.

## Configuration du projet
### Prérequis
* Python 3.x installé sur votre machine.
* Azure CLI configuré avec un compte Azure valide.
* Accès à un conteneur Azure Blob Storage contenant les fichiers à traiter.
* Bash (pour exécuter les scripts).

### Installation
1. Clonez le dépôt
```
git clone https://github.com/SimonRqrt/adventureworks.git
```

2. Installez les dépendances Python :

```
pip install -r requirements.txt
```

3. Créez un fichier .env dans le répertoire du projet et ajoutez vos informations Azure :

```
STORAGE_ACCOUNT_NAME=VotreNomDeCompteAzure
STORAGE_ACCOUNT_KEY=VotreCléDeCompteAzure
DIRECTORY=VotreRépertoireDansLeConteneur
AZURE_CONTAINER_NAME=NomDuConteneur
```

4. Exécutez le script sas_generation.sh pour générer un SAS token et mettre à jour le fichier .env :

```
bash sas_generation.sh
```

## Guide d'utilisation
### Étapes principales
1. Exécutez le script principal start.sh :
```
bash start.sh
```

Ce script effectue les étapes suivantes :

- Télécharge les fichiers Parquet depuis Azure Blob Storage.
- Traite chaque fichier pour extraire les images et métadonnées.
- Organise les fichiers localement dans le dossier downloads.

2. Les fichiers générés sont structurés ainsi :

```
downloads/
├── product_eval/
│   ├── images/
│   │   ├── nom_du_fichier_parquet/
│   │   │   ├── image_0_key1.png
│   │   │   ├── image_1_key2.png
│   │   │   └── ...
│   ├── metadata/
│   │   ├── nom_du_fichier_parquet_metadata.csv
│   │   └── ...
```

Une fois le traitement terminé, les fichiers Parquet originaux sont supprimés pour économiser de l’espace.

## Exemples de commande
### Récupérer un token SAS manuellement :
```
bash sas_generation.sh
```
### Exécuter une partie de l'extraction (sur les fichiers parquet par exemple) :
```
python3 parquet.py
```

## Structure du projet
```
azure-blob-downloader/
├── .env.example        # Exemple de configuration pour votre environnement
├── sas_generation.sh   # Script pour générer un SAS token
├── main.py             # Script principal pour le traitement des fichiers
├── requirements.txt    # Liste des dépendances Python
├── start.sh            # Script pour lancer tout le pipeline
├── README.md           # Documentation du projet
└── downloads/          # Répertoire où les données sont sauvegardées
```

## Points importants
- Expirations du token SAS : Assurez-vous que le token SAS a une durée de validité suffisante pour éviter des erreurs d’authentification.
- Compatibilité : Testé sur Linux/MacOS. Les utilisateurs Windows doivent adapter les commandes Bash.
