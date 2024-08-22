from pymongo import MongoClient
from pprint import pprint
import re 

print("_Connexion à la base de données_","\n")

print("\n(a) Pour se connecter à MongoDB via pymongo, ajoutez l'authentification aux lignes de codes suivantes puis lancez-les :")
client = MongoClient(
    host = "127.0.0.1",
    port = 27017,
    username = "datascientest",
    password = "dst123"
)
print(client)

print("\n(b) Afficher la liste des bases de données disponibles.")
print(client.list_database_names())

print("\n(c) Afficher la liste des collections disponibles dans cette base de données.")
print(client["sample"].list_collection_names())

print("\n(d) Afficher un des documents de cette collection.")
pprint(client["sample"]["books"].find_one())

print("\n(e) Afficher le nombre de documents dans cette collection")
pprint(client["sample"]["books"].count_documents({}))

print("\n_Exploration de la base_")

print("\n(a) Afficher le nombre de livres avec plus de 400 pages, affichez ensuite le nombre de livres ayant plus de 400 pages ET qui sont publiés.") 
pprint(list(client["sample"]["books"].aggregate([{"$match":{"pageCount":{"$gt":400}}},{"$count":"nbrDocuments"}])))
pprint(list(client["sample"]["books"].aggregate([{"$match":{"$and":[{"pageCount":{"$gt":400}},{"status":"PUBLISH"}]}},{"$count":"nbrDocuments"}])))

print("\n(b) Afficher le nombre de livres ayant le mot-clé Android dans leur description (brève ou longue). ")
exp = re.compile("Android")
pprint(len(list(client["sample"]["books"].find({"$or":[{"shortDescription":exp}, {"longDescription":exp}]}))))

print("\n(c) Chaque document possède un attribut categories qui est une liste. Vous devez grouper tous les documents en un à l'aide de l'opérateur $group. Puis, à l'aide de l'opérateur $addToSet, créez 2 set à partir des catégories contenus dans la liste categories selon leur index 0 ou 1. Pour cibler, les catégories utilisez l'opérateur $arrayElemAt.")
pprint(list(client["sample"]["books"].aggregate([{"$group":{"_id":0,"Categorie1":{"$addToSet":{"$arrayElemAt":["$categories",0]}},"Categorie2":{"$addToSet":{"$arrayElemAt":["$categories",1]}}}},{"$project":{"_id":0,"Categorie1":1,"Categorie2":1}}])))

print("\n(d) Afficher le nombre de livres qui contiennent des noms de langages suivant dans leur description longue : Python, Java, C++, Scala. On pourra s'appuyer sur des expressions régulières et une condition or.")
exp = re.compile("Python|Java|C\\+\\+|Scala")
pprint(len(list(client["sample"]["books"].find({"longDescription":exp}))))

print("\n(e) Afficher diverses informations statistiques sur notre bases de données : nombre maximal, minimal, et moyen de pages par catégorie. On utilisera une pipeline d'aggregation, le mot clef $group, ainsi que les accumulateurs appropriés. N'oubliez pas d'aller voir \"$unwind\" pour ce problème.")
pprint(list(client["sample"]["books"].aggregate([{"$unwind":"$categories"},{"$group":{"_id":"$categories","maxPages":{"$max":"$pageCount"},"minPages":{"$min":"$pageCount"},"avgPages":{"$avg":"$pageCount"}}}])))

print("\n(f) Via une pipeline d'aggrégation, Créer de nouvelles variables en extrayant info depuis l'attribut dates : année, mois, jour. On rajoutera une condition pour filtrer seulement les livres publiés après 2009. N'affichez que les 20 premiers.")
pprint(list(client["sample"]["books"].aggregate([{"$project":{"title":1,"année":{"$year":"$publishedDate"},"mois":{"$month":"$publishedDate"},"jour":{"$dayOfMonth":"$publishedDate"}}},{"$match":{"année":{"$gt":2009}}}])))

print("\n(g) À partir de la liste des auteurs, créez de nouveaux attributs (author_1, author_2 ... author_n). Observez le comportement de \"$arrayElemAt\". N'affichez que les 20 premiers dans l'ordre chronologique.")
pprint(list(client["sample"]["books"].aggregate([{"$project":{"année":{"$year":"$publishedDate"},"mois":{"$month":"$publishedDate"},"jour":{"$dayOfMonth":"$publishedDate"},"author_1":{"$arrayElemAt":["$authors",0]},"author_2":{"$arrayElemAt":["$authors",1]},"author_3":{"$arrayElemAt":["$authors",2]}}},{"$match":{"année":{"$ne":None}}},{"$sort":{"année":1,"mois":1,"jour":1}},{"$limit":20}])))

print("\n(h) En s'inspirant de la requête précédente, créer une colonne contenant le nom du premier auteur, puis agréger selon cette colonne pour obtenir le nombre d'articles pour chaque premier auteur. Afficher le nombre de publications pour les 10 premiers auteurs les plus prolifiques. On pourra utiliser un pipeline d'agrégation avec les mots-clefs $group, $sort, $limit.")
pprint(list(client["sample"]["books"].aggregate([{"$match":{"status":"PUBLISH"}},{"$project":{"author_1":{"$arrayElemAt":["$authors",0]}}},{"$group":{"_id":"$author_1", "nbPb":{"$sum":1}}},{"$sort":{"nbPb":-1}},{"$limit":10}])))
