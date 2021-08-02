# see https://creodias.eu/forum/-/message_boards/message/155867

'''
dernière version : 'GH'
exemple:

python3 CREODIAS_download.py -o /home/dossierDeDepot -i /home/dossier/compteCREODIAS.txt -y 2019 -shp /home/dossier/shapefile.shp -ec 20

effectu dans cet ordre:
-selection cycle annuel
-tri des image avec 100% de nuage
-téléchargement es zip
-trie des images avec nuage dans l'emprise (-ec     ->pour emprise couverture(nuageuse))
-concaénation


example initiale: 

python3 CREODIAS_download.py -sat S2 
			      -p L1C 
			      -s 2019-04-03 
			      -e 2019-06-04 
			      -x -0.425 -0.732 44.559 44.875 
			      -c 20 
			      -o /home/guillaume/Documents/pour_stage_dev_2021/mission_10_downloadS2/creodias-sentinel-download-master 
			      -i /home/guillaume/Documents/pour_stage_dev_2021/mission_10_downloadS2/creodias-sentinel-download-master/compteCREODIAS.txt

'''

import os
import requests
import json
import argparse
from tqdm import tqdm
import re
import sys
import datetime
import geopandas
from shapely.geometry import Polygon,shape,mapping
import glob
import subprocess

def SHPtoList(shp_path):
	shp=geopandas.read_file(shp_path)#load shapefile
	emprise=shp['geometry']
	#print(emprise[0])
	return emprise[0].bounds
	

def Curently(annee):
	'''
	selectionne de octobre à octobre si cycle en cours alors on prendra octobre à octobre du cycle d'avant
	'''
	date_now=datetime.date.today()
	#print('actuelle :',date_now)
	annee=int(annee)
	date_start=datetime.date(annee-1,10,1)
	date_end=datetime.date(annee,10,1)
	it=1
	while date_end>date_now:
		date_start=datetime.date(annee-(it+1),10,1)
		date_end=datetime.date(annee-it,10,1)
		it=it+1
	#print('commence le :',date_start)
	#print('finis le :',date_end)
	return date_start, date_end
		
	

def get_keycloak_token(username, password):
    h = {
    'Content-Type': 'application/x-www-form-urlencoded'
    }
    d = {
    'client_id': 'CLOUDFERRO_PUBLIC',
    'password': password,
    'username': username,
    'grant_type': 'password'
    }
    resp = requests.post('https://auth.creodias.eu/auth/realms/dias/protocol/openid-connect/token', data=d, headers=h)
    #print(resp.status_code)
    try:
        token = json.loads(resp.content.decode('utf-8'))['access_token']
    except KeyError:
        print("Can't obtain a token (check username/password), exiting.")
        sys.exit()
    #print(token)
    return token


def CREODIAS_sentinel_download(sat, productType, start_date, end_date, extent, cloud_max, outFolder, IDfile):

    CREODIAS_finder_url = "https://finder.creodias.eu"
    
    # recuperation login
    f = open(IDfile,'r')
    login = f.readlines()
    f.close()
    username = login[0][:-1]
    password = login[1][:-1]
    
    # make WKT extent search
    lonmin = extent[0]
    lonmax = extent[2]
    latmin = extent[1]
    latmax = extent[3]
    WKTpolygon = 'POLYGON((%s %s,%s %s,%s %s,%s %s,%s %s))' % (lonmin, latmax, lonmin, latmin, lonmax, latmin, lonmax, latmax, lonmin, latmax)

    #print(WKTpolygon)
    WKTpolygon = WKTpolygon.replace(' ', '+')
    WKTpolygon = WKTpolygon.replace(',', '%2C')
    
    # set sat name
    if sat.lower() in ['sentinel3', 's3']:
        sat = 'Sentinel3'
        spec_string = 'instrument=OL&productType=%s&timeliness=Non+Time+Critical' % productType
    elif sat.lower() in ['sentinel2', 's2']:
        sat = 'Sentinel2'
        if productType == 'L1C':
            productType = 'LEVEL1C'
        elif productType == 'L2A':
            productType = 'LEVEL2A'
        spec_string = 'processingLevel=%s' % productType
        
    # make date search string
    date_string = 'startDate=%sT00:00:00Z&completionDate=%sT23:59:59Z' % (start_date, end_date)
    date_string = date_string.replace(':', '%3A')
    
    #make cloud cover string
    if sat == 'Sentinel2':
        cloud_string = 'cloudCover=[0,%s]' % cloud_max
        cloud_string = cloud_string.replace('[', '%5B')
        cloud_string = cloud_string.replace(',', '%2C')
        cloud_string = cloud_string.replace(']', '%5D')
    elif sat == 'Sentinel3':
        cloud_string = ''
        print ('filter by cloud coverage does not apply on Sentinel3 data')
    
    # build finder api url    
    url = ['%s/resto/api/collections/%s/search.json?maxRecords=100' % (CREODIAS_finder_url, sat),
           date_string,
           cloud_string,
           spec_string,
           'geometry=%s' % WKTpolygon,
           'sortParam=startDate',
           'sortOrder=descending',
           'status=all',
           'dataset=ESA-DATASET']
    finder_api_url = ('&').join(url)
 
    # send request to url
    response = requests.get(finder_api_url)
    
    # change working path to outFolder
    os.chdir(outFolder)
    all_download=[]
    #print (response)
    for feature in json.loads(response.content.decode('utf-8'))['features']:
        token = get_keycloak_token(username, password)
        download_url = feature['properties']['services']['download']['url']
        download_url = download_url + '?token=' + token
        total_size = feature['properties']['services']['download']['size']
        title = feature['properties']['title']
        #print('###############################')
        #print(feature['properties']['title'].split('.')[0]+'.zip')
        filename = title + '.zip'
        all_download.append(feature['properties']['title'].split('.')[0]+'.zip')
        if not os.path.exists(os.path.join(outFolder, title[:-5] + '.zip')) and not os.path.exists(os.path.join(outFolder, title)): # if not already downloaded or not already downloaded and unzipped
            r = requests.get(download_url, stream=True)
            if "Content-Disposition" in r.headers.keys():
                filename = re.findall("filename=(.+)", r.headers["Content-Disposition"])[0]
            # Total size in bytes.
            total_size = int(r.headers.get('content-length', 0))
            if total_size <= 100:
                #print(r.text)
                sys.exit("Please try again in few moments.")
            block_size = 1024 #1 Kibibyte
            print('downloading:', filename)
            t=tqdm(total=total_size, unit='iB', unit_scale=True)
            with open(filename, 'wb') as f:
                for data in r.iter_content(block_size):
                    t.update(len(data))
                    f.write(data)
            t.close()
            if total_size != 0 and t.n != total_size:
                print("ERROR, something went wrong")
    return all_download

def Cloud_remover(path,prop_nuage,liste_des_noms,shp_path):
	from zipfile import ZipFile
	import matplotlib.pyplot as plt
	import codecs
	import shutil
	
	moyenne_nuageuse=[]
	save=[]
	suprimer=0
	prop_nuage=int(prop_nuage)
	for name in liste_des_noms:
		with ZipFile(path+'/'+name,'r') as zipObj:
			zipObj.extractall()
		pathToCheck=path+'/'+os.path.splitext(name)[0]+'.SAFE/GRANULE/**/QI_DATA/MSK_CLOUDS_B00.gml'
		for file in glob.glob(pathToCheck):
			check_validity=0
			f=codecs.open(file,encoding='utf-8',errors='strict')
			for line in f:
				check_validity=check_validity+1
			if check_validity>5:	
				cloud=geopandas.read_file(file)
			else:
				cloud=geopandas.GeoDataFrame()
				suprimer=suprimer+1
				#os.remove(path+'/'+os.path.splitext(name)[0]+'.SAFE')
				shutil.rmtree(path+'/'+os.path.splitext(name)[0]+'.SAFE')
				
		if not cloud.empty:
			emprise=geopandas.read_file(shp_path)
			if cloud.crs!=emprise.crs:
				emprise=emprise.copy()
				emprise=emprise.to_crs({'init':cloud.crs})
				
			emprise=emprise['geometry']
			cloud=cloud['geometry']
			clipped=geopandas.clip(cloud,emprise)
			'''
			fig,ax=plt.subplots()
			clipped.plot(ax=ax,color='red')
			cloud.boundary.plot(ax=ax,color='green')
			emprise.boundary.plot(ax=ax)
			plt.show()
			'''
			surface_emprise=emprise.area
			surface_nuage_dans_emprise=clipped.area
			prop=(sum(surface_nuage_dans_emprise)*100)/surface_emprise
			'''
			print('surface total:',sum(surface_emprise))
			print('surface nuageuse :',sum(surface_nuage_dans_emprise))
			print('proportion de nuages:',sum(prop))
			print('proporton max:',prop_nuage)
			'''
			if int(prop) >= prop_nuage:
				suprimer=suprimer+1
				#os.remove(path+'/'+os.path.splitext(name)[0]+'.SAFE')
				shutil.rmtree(path+'/'+os.path.splitext(name)[0]+'.SAFE')
			else:
				save.append(name)
			moyenne_nuageuse.append(prop)
		os.remove(path+'/'+name)
		
	return moyenne_nuageuse,suprimer,save
	
def otb_concatenate_band_wrapper(in_img_list, out_img, ram):
    """
    concatenation bande s2 en uint16
    """
    #cmd = ['source', "/home/nicodebo/.local/share/dependencies/otb/otbenv.profile" , '&&', 'otbcli_ConcatenateImages', '-il']
    cmd = ['otbcli_ConcatenateImages' , '-il']
    cmd.extend(in_img_list)
    cmd.extend(['-out', out_img, 'uint16', '-ram', str(ram)])
    print(" ".join(cmd))
    subprocess.check_call(" ".join(cmd), shell=True, executable='/bin/bash')


def prepare_s2(img_folder, out_dir, ram):
    """
    preparation des images full s2
    img_folder: chemin d'accès des données S2
    out_dir: chemin ou seront enregistrer les images concatenées
    """
    tiles = glob.glob(os.path.join(img_folder, '*.SAFE'))
    print(tiles)
    for tile in tiles:
        outfolder = os.path.join(out_dir)
        os.makedirs(outfolder, exist_ok=True)
        prefix = os.path.splitext(os.path.basename(tile))[0]
        out_img = os.path.join(outfolder, '{}.tif'.format(prefix))

        in_img_list = []
        in_img_list = glob.glob(os.path.join(tile, '**', '*B02.jp2'), recursive=True)
        in_img_list += glob.glob(os.path.join(tile, '**', '*B03.jp2'), recursive=True)
        in_img_list += glob.glob(os.path.join(tile, '**', '*B04.jp2'), recursive=True)
        in_img_list += glob.glob(os.path.join(tile, '**', '*B08.jp2'), recursive=True)

        if not os.path.isfile(out_img):
            otb_concatenate_band_wrapper(in_img_list, out_img, ram)

def concat(restants,img_folder):
    out_dir = img_folder
    prepare_s2(img_folder, out_dir, ram=256)

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description="download Sentinel-2 or Sentinel-3 data from CREODIAS repositery. WARNING does not handle more than 100 data to be downloaded by once.")
    parser.add_argument(
        "--sat",
        "-sat",
        help="satellite name. S2 or S3 default S2",
        required=False,
        default='S2'
    )
    parser.add_argument(
        "--productType",
        "-p",
        help="product type, L1C or L2A for S2, EFR or WFR for S3 default L1C",
        required=False,
        default='L1C'
    )
    '''
    parser.add_argument(
        "--start_date",
        "-s",
        help="starting search date, format YYYY-MM-DD",
        required=True,
    )
    parser.add_argument(
        "--end_date",
        "-e",
        help="last search date, format YYYY-MM-DD",
        required=True,
    )
    parser.add_argument(
        "--extent",
        "-x",
        nargs = '+',
        help="geographical extent of search, format = lonmin, lonmax, latmin, latmax",
        required=True,
    )
    '''
    parser.add_argument(
        "--cloud_max",
        "-c",
        help="max percentage of cloud cover default 90",
        required=False,
        default=90      #permets de faire un premier filtre car une image avec 100% de nuage ne sert à rien
    )
    parser.add_argument(
        "--outFolder",
        "-o",
        help="path to folder where data are stored",
        required=True,
    )
    parser.add_argument(
        "--IDfile",
        "-i",
        help="path to text file containing CREODIAS identifiers. First line = username, second line = password",
        required=True,
    )
    
    parser.add_argument(
        "--years",
        "-y",
        help="year wanted format XXXX default curently",
        required=False,
        default=datetime.date.today().year
    )
    
    parser.add_argument(
        "--Nuage_emprise",
        "-ec",
        help="max percentage of cloud cover into geographical extent default 20",
        required=False,
        default=20
    )
    parser.add_argument(
        "--shp_path",
        "-shp",
        help="emprise en .shp path",
        required=True,
    )
    args = parser.parse_args()
    
    
    extent=SHPtoList(args.shp_path)
    print(extent)
    date_start,date_end=Curently(args.years)
    #date_start='2019-03-28'
    #date_end='2019-04-04'
    print(date_start,date_end)
    
    liste_des_noms=CREODIAS_sentinel_download(args.sat, args.productType, date_start, date_end, extent, args.cloud_max, args.outFolder, args.IDfile)
    #print(liste_des_noms)
    moy,sup,restants=Cloud_remover(args.outFolder,args.Nuage_emprise,liste_des_noms,args.shp_path)
    concat(restants,args.outFolder)
    print('################')
    print("nombres d'images :",len(moy)+1)
    print("nombre d'images conservées:" , len(moy)-sup+1)
    print('Moyenne nuageuse du cycle :', (sum(moy)/len(moy))[0])
    print('################')
