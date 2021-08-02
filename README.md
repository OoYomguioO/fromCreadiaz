prend en entrée une emprise shapefile et renvoie toutes les images Sentinelles comportant l'emprise 
du moment qu'il n'y a pas trop de nuage au seins de l'emprise pas uniquement de l'image

# exemple

python3 CREODIAS_download.py -o /home/dossierDeDepot -i /home/dossier/compteCREODIAS.txt -y 2019 -shp /home/dossier/shapefile.shp -ec 20
