# SparkProject
School pyspark project

## Requirements
* Python 3.6+
* PySpark 3.1.0+
* Download [full.csv](https://www.kaggle.com/dhruvildave/github-commit-messages-dataset/version/3?select=full.csv) here and place it into data directory.

## Run it
### Install dependencies
```shell
pip install pyspark
```

### Performances and time
With cached data, the following results are obtained (on core i9-9900k):
```
--- Total time : 0:01:04.934484 seconds
```

With non-cached data, the following results are obtained (on core i9-9900k):
````
--- Total time : 0:00:36.262321 seconds
````

## Wording - French version (fr)

Récupérez le dataset full.csv du projet GitHub Commit Messages sur Kaggle.
Votre application Spark devra effectuer les actions suivantes sur ce dataset :
1. Afficher dans la console les 10 projets Github pour lesquels il y a eu le plus de
commit.
2. Afficher dans la console le plus gros contributeur (la personne qui a fait le plus de
commit) du projet apache/spark.
3. Afficher dans la console les plus gros contributeurs du projet apache/spark sur les 6
derniers mois. Le code doit être générique, si on le relance dans 6 mois il devra
donner les plus gros contributeurs des 6 prochains mois, pas de date en dur dans le
code 😉. Pour la conversion vous pouvez vous référer à cette documentation.
4. Afficher dans la console les 10 mots qui reviennent le plus dans les messages de
commit sur l’ensemble des projets. Vous prendrez soin d’éliminer de la liste les
stopwords pour ne pas les prendre en compte. Vous êtes libre d’utiliser votre propre
liste de stopwords, vous pouvez sinon trouver des listes ici.
