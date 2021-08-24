from PIL import Image
from csv import writer
from Google import Create_Service
from Google import MediaIoBaseDownload
import io, os, sys, uuid, csv
import numpy as np

IMAGES_FOLDER_NAME = 'images'
PROCESSED_IMAGES_FOLDER_NAME = 'processed'
IMAGES_TABLE_CSV_NAME = 'image_table.csv'
PAGE_SIZE_CONST = 100

NEW_SIZE = (160, 120)

FILE_SEPERATOR = os.path.sep
IMAGE_FILE_IDS = []
CLIENT_SECRET_FILE = 'client_secret_quickstart.json'
API_NAME = 'drive'
API_VERSION = 'v3'
SCOPES = ['https://www.googleapis.com/auth/drive']
nextPageToken = 'nextPageToken'

def folder_init():
    images_path = os.getcwd() + FILE_SEPERATOR + IMAGES_FOLDER_NAME
    processed_images_path = os.getcwd() + FILE_SEPERATOR + PROCESSED_IMAGES_FOLDER_NAME
    if not os.path.exists(images_path):
        os.makedirs(images_path)
    if not os.path.exists(processed_images_path):
        os.makedirs(processed_images_path)

def download_convert_persist():
    fileNameDict = {}
    newFileList = []
    for f in IMAGE_FILE_IDS:
        img = f[0]
        #print(f)
        print(u'{0} ({1})'.format(img['imageMediaMetadata']['time'], img['name']))
        file_id = img['id']
        request = service.files().get_media(fileId=file_id)
        newFileName = img['name'].split(".")[0] + "_"  + str(uuid.uuid4())[:8] + "_" + img['imageMediaMetadata']['time'].replace(":", "_")
        newFileName = newFileName.replace(" ", "_")
        file_path = download_file_from_drive_return_abs_path(file_id, os.getcwd() + FILE_SEPERATOR + IMAGES_FOLDER_NAME, img['name'])
        new_file_path = os.getcwd() + FILE_SEPERATOR + PROCESSED_IMAGES_FOLDER_NAME + FILE_SEPERATOR + newFileName + '.' + img['name'].split(".")[1]
        convert_to_120_120_grayscale(file_path, new_file_path)
        newFileList.append(newFileName)

        append_processed_list_to_remote_csv(newFileList)

def append_processed_list_to_remote_csv(newFileList):
    id = find_file_name_like(IMAGES_TABLE_CSV_NAME)
    if id == None:
        print('Could not find ' + IMAGES_TABLE_CSV_NAME + ' in google drive!')
    else
        csv_local_path = download_file_from_drive_return_abs_path(id, os.getcwd(), IMAGES_TABLE_CSV_NAME)
    
    with open(csv_local_path, 'a+', newline='') as write_obj:
        csv_writer = writer(write_obj)
        # Add contents of list as last row in the csv file
        csv_writer.writerow(newFileList)
    

def find_file_name_like(file_name):
    results = service.files().list(
        pageSize=PAGE_SIZE_CONST, fields="nextPageToken, files(id, name)").execute()
    items = results.get('files', [])

    if not items:
        return None
    else:
        print('Files:')
        for item in items:
            print(item['name'])
            #find images folder
            if item['name'].lower() == file_name.lower() :
                print('found ' +file_name )
                return item['id']
    return None

def download_file_from_drive_return_abs_path(drive_file_id, new_file_location, new_file_name):
    request = service.files().get_media(fileId=drive_file_id)
    file_path = new_file_location + FILE_SEPERATOR + new_file_name
    fh = io.FileIO(file_path, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        try:
            status, done = downloader.next_chunk()
        except:
            fh.close()
            os.remove(file_path)
            sys.exit(1)
        print(f'\rDownload {int(status.progress() * 100)}%.', end='')
        sys.stdout.flush()
    return file_path

def convert_to_120_120_grayscale(path, new_file_path):
    print(path)
    full_filename = os.path.basename(path)
    base_filename = path.split(".")[0]
    base_fileformat = path.split(".")[1]
    print(base_fileformat)
    # Remove banner\r\n",
    im = Image.open(path)
    a = np.array(im)
    a = a[:-229, :]
    im = Image.fromarray(a)
    # Convert to 160x120 size
    im = im.resize(NEW_SIZE)
    # Convert to grayscale
    im = im.convert('L')
    # Save processed
    #newimfile = new_file_path + FILE_SEPERATOR + "\{}".format(timestamp)+"."+base_fileformat
    print(new_file_path)
    im.save(new_file_path)
    return True

def upload_to_drive(file_path)
    # TO DO
  
service = Create_Service(CLIENT_SECRET_FILE, API_NAME, API_VERSION, SCOPES)
folder_init()
id = find_file_name_like(IMAGES_FOLDER_NAME)
while nextPageToken != None and id != None:
    if nextPageToken.lower() == 'nextPageToken'.lower():
        request = service.files().list(q = "'" + id + "' in parents", pageSize=PAGE_SIZE_CONST, fields="nextPageToken, files(id,name,imageMediaMetadata(time))").execute()
    else:
        request = service.files().list(q = "'" + id + "' in parents", pageSize=PAGE_SIZE_CONST, pageToken=nextPageToken, fields="nextPageToken, files(id,name,imageMediaMetadata(time))").execute()
    items2 = request.get('files', [])
    IMAGE_FILE_IDS.append(items2)
    #nextPageToken = request.get('nextPageToken', None)
    nextPageToken = None

download_convert_persist()