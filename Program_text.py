import json, xmltodict
import os
from xml.parsers.expat import ExpatError
import subprocess
from pymongo import MongoClient
import xml.etree.ElementTree as ET
import re

#-------------------РАБОТА С БД------------------------------#
# Подключение к бд
client = MongoClient('127.0.0.1', 27017)

# Объявление базы данных
db = client.admin

# Объявление коллекции
Patents = db.Patents

#------------------------------------------------------------#



#------------------ОБЪЯВЛЕНИЕ ПЕРЕМЕННЫХ ДЛЯ ЗАПИСИ В БД------------------------------#

Autor_ru = []
Autor_eu = []
Facts = []
AOC = []
Index = []
Copyright_holder_ru = []
Copyright_holder_eu = []
Class_index = []
Cit_list = []
Check_list_AOC = ["изобретение относится", "изобретение может", "полезная модель", "полезной моделью",
                  "настоящее изобретение", "технический результат", "техническим результатом", "устройство относится",
                  "задачей изобретения", "цель изобретения", "целью изобретения", "целью настоящего изобретения",
                  "задача на решение", "группа изобретений"
]

#-------------------------------------------------------------------------------------#



#------------------ОБЪЯВЛЕНИЕ ПЕРЕМЕННЫХ ПУТЕЙ------------------------------#
pathOutText = '/home/aruchok/Project/Parser/Out/'
pathTomitaParser = '/home/aruchok/tomita-parser/build/project/'
pathFacts = '/home/aruchok/Project/Parser/OutFacts/'
pathDebug = '/home/aruchok/tomita-parser/build/project/'
pathOutDebug = '/home/aruchok/Project/Parser/OutHtml/'
pathFactsXMLTomita = '/home/aruchok/tomita-parser/build/project/'

#path to the folder holding the XML
directoryXML = '/home/aruchok/Project/Parser/XML/'

#--------------------------------------------------------------------------#

# Добавление в бд
def insert_document(collection, data):

     return collection.insert_one(data).inserted_id

# Получение элемента
def find_document(collection, elements, multiple=False):
    if multiple:
        results = collection.find(elements)
        return [r for r in results]
    else:
        return collection.find_one(elements)

# Обновление данных
def update_document(collection, query_elements, new_values):

    collection.update_one(query_elements, {'$push': new_values})


def add_AOC(collection, query_elements, catigory, new_values):

    # collection.update_one(query_elements, {'$addToSet': {catigory: new_values}})
    collection.update_one(query_elements, {'Index': mystr, 'AOC': []}, {'$addToSet': {catigory: new_values}})


#Парсинг xml в json
for filename in os.listdir(directoryXML):
    if filename.endswith(".xml"):

        #parse the content of each file using xmltodict
        try:
            x = xmltodict.parse(open(directoryXML + '/' + filename, encoding='utf-8').read())
        except ExpatError:
            x = xmltodict.parse(open(directoryXML + '/' + filename, encoding='utf-8').read()[3:])
        j = json.dumps(x, ensure_ascii=False)
        filename = filename.replace('.xml', '')

        # Сохранение в директорию .json файла
        output_file = open('/home/aruchok/Project/Parser/Json/' + filename + '.json', 'w', encoding='utf8')
        output_file.write(j)


# Алгоритм извлечения информации для идентификации патента из патентных данных в Json формате 
directoryXML = '/home/aruchok/Project/Parser/Json/'
for filename in os.listdir(directoryXML):
    if filename.endswith(".json"):
        try:
            with open(directoryXML + filename, 'r', encoding='utf-8') as read_file:
                file_json = json.load(read_file)

            mystr = re.sub(r"[.json]", "", filename)

            # Вывод названия на русском и запись в файл
            Title_ru = file_json['ru-patent-document']['SDOBI'][0]['B500']['ru-b540']['ru-b542']
            print(Title_ru)
            WriteInFile = open('/home/aruchok/Project/Parser/Out/' + mystr + '.txt', "w")

            # Вывод названия на английском и запись в файл
            Title_eu = file_json['ru-patent-document']['SDOBI'][1]['B500']['ru-b540']['ru-b542']

            # Уникальный номер патента
            Num_pat = file_json['ru-patent-document']['SDOBI'][0]['B100']['B190'] + \
                      file_json['ru-patent-document']['SDOBI'][0]['B100']['B110'] + \
                      file_json['ru-patent-document']['SDOBI'][0]['B100']['B130']

            #Классификационый уровень
            l = len(file_json['ru-patent-document']['SDOBI'][0]['B500']['B510EP']['classification-ipcr'])
            i = 0
            while i < l:
                try:
                    if l == 13:
                        Class_index.append(file_json['ru-patent-document']['SDOBI'][0]['B500']['B510EP']['classification-ipcr']['section'] + \
                          file_json['ru-patent-document']['SDOBI'][0]['B500']['B510EP']['classification-ipcr']['class'] + \
                          file_json['ru-patent-document']['SDOBI'][0]['B500']['B510EP']['classification-ipcr']['subclass'] + ' ' + \
                          file_json['ru-patent-document']['SDOBI'][0]['B500']['B510EP']['classification-ipcr']['main-group'] + '/' + \
                          file_json['ru-patent-document']['SDOBI'][0]['B500']['B510EP']['classification-ipcr']['subgroup'])
                        break
                          
                    Class_index.append(file_json['ru-patent-document']['SDOBI'][0]['B500']['B510EP']['classification-ipcr'][i]['section'] + \
                          file_json['ru-patent-document']['SDOBI'][0]['B500']['B510EP']['classification-ipcr'][i]['class'] + \
                          file_json['ru-patent-document']['SDOBI'][0]['B500']['B510EP']['classification-ipcr'][i]['subclass'] + ' ' + \
                          file_json['ru-patent-document']['SDOBI'][0]['B500']['B510EP']['classification-ipcr'][i]['main-group'] + '/' + \
                          file_json['ru-patent-document']['SDOBI'][0]['B500']['B510EP']['classification-ipcr'][i]['subgroup'])
                except KeyError:
                    print('')
                i += 1

            print(mystr)

            # Адрес
            Adress_ru = file_json['ru-patent-document']['SDOBI'][0]['B900']['ru-b980']
            Adress_eu = file_json['ru-patent-document']['SDOBI'][1]['B900']['ru-b980']

            # Получаем колличество полей
            l = len(file_json['ru-patent-document']['SDOBI'][0]['B700']['B720']['B721'])

            # Перебираем и записываем эти поля в консоль и в файл (Автор ру и еу)
            i = 0
            while i < l:
                try:
                    if l == 1:
                        Autor_ru.append(file_json['ru-patent-document']['SDOBI'][0]['B700']['B720']['B721']['ru-name-text'])
                    Autor_ru.append(file_json['ru-patent-document']['SDOBI'][0]['B700']['B720']['B721'][i]['ru-name-text'])
                except KeyError:
                    # WriteInFile.write('\n')
                    print('')
                i += 1


            i = 0
            while i < l:
                try:
                    if l == 1:
                        Autor_eu.append(file_json['ru-patent-document']['SDOBI'][1]['B700']['B720']['B721']['ru-name-text'])
                    Autor_eu.append(file_json['ru-patent-document']['SDOBI'][1]['B700']['B720']['B721'][i]['ru-name-text']) 
                except KeyError:
                    # WriteInFile.write('\n')
                    print('')
                i += 1
          
            # Правообладатель на обоих языках
            l = len(file_json['ru-patent-document']['SDOBI'][0]['B700']['B730']['B731'])
            i = 0
            while i < l:
                try:
                    if l == 1:
                        Copyright_holder_ru.append(file_json['ru-patent-document']['SDOBI'][0]['B700']['B730']['B731']['ru-name-text'])
                    Copyright_holder_ru.append(file_json['ru-patent-document']['SDOBI'][0]['B700']['B730']['B731'][i]['ru-name-text'])
                except KeyError:
                    print('')
                i += 1

            i = 0
            while i < l:
                try:
                    if l == 1:
                        Copyright_holder_eu.append(file_json['ru-patent-document']['SDOBI'][1]['B700']['B730']['B731']['ru-name-text'])
                    Copyright_holder_eu.append(file_json['ru-patent-document']['SDOBI'][1]['B700']['B730']['B731'][i]['ru-name-text'])
                except KeyError:
                    # WriteInFile.write('\n')
                    print('')
                i += 1
            # print(Copyright_holder_eu)

            # Дата публикации
            Date_pub = file_json['ru-patent-document']['SDOBI'][0]['B100']['B140']['date']

            date_temp = Date_pub[0:4] + '.' + Date_pub[4:6] + '.' + Date_pub[6:8]
            Date_pub = date_temp
            # print(Date_pub)

            # Список цитирования
            l = len(file_json['ru-patent-document']['SDOBI'][0]['B500']['B560']['ru-b560'])
            i = 0
            while i < l:
                try:
                    if l == 2:
                        Cit_list.append(file_json['ru-patent-document']['SDOBI'][0]['B500']['B560']['ru-b560']['#text'])
                        Cit_list.append(file_json['ru-patent-document']['SDOBI'][0]['B500']['B560']['ru-b560']['p']['#text'])
                        break
                    if l > 10:
                        Cit_list.append(file_json['ru-patent-document']['SDOBI'][0]['B500']['B560']['ru-b560'])
                        break
                except KeyError:
                    # WriteInFile.write('\n')
                    print('')
                i += 1


            # mystr = re.sub(r"[.txt]", "", filename)

            # Check_list_AOC
            len_check = len(Check_list_AOC)
            # Вывод предложений description и запсь в файл
            l = len(file_json['ru-patent-document']['description']['p'])
            i = 0
            n = 0
            while i < l:
                try:
                    Temp = file_json['ru-patent-document']['description']['p'][i]['#text']
                    while n < len_check:
                        if str.find(Temp, Check_list_AOC[n], 0) != -1:
                            print(Temp)
                            WriteInFile.write(Temp)
                            # WriteInFile.write('\n')
                            break

                        Check_AOC = Check_list_AOC[n].capitalize()

                        if str.find(Temp, Check_AOC, 0) != -1:
                            print(Temp)
                            WriteInFile.write(Temp)
                            # WriteInFile.write('\n')
                            break

                        # Check_AOC = Check_list_AOC[n].lower()
                        n += 1
                    n = 0
                except KeyError:
                    print(' ')
                i += 1
            print('\n')
            
            post = {
                'Number_patent': Num_pat,
                'Title_ru': Title_ru,
                'Title_eu': Title_eu,
                'Class_index': [Class_index],

                'Autor_ru': [Autor_ru],
                'Autor_eu': [Autor_eu],

                'Copyright_holder_ru': [Copyright_holder_ru],
                'Copyright_holder_eu': [Copyright_holder_eu],

                'Publication_date_(EMD)': Date_pub,
                'Citation_list': Cit_list,

                'Index': mystr,
                'AOC': [AOC]            
            }

            insert_document(Patents, post)

            Autor_ru.clear()
            Autor_eu.clear()
            Copyright_holder_ru.clear()
            Copyright_holder_eu.clear()
            Class_index.clear()
            Cit_list.clear()

            WriteInFile.close()

        except KeyError:
            continue

# Алгоритм извлечения фактов из текстового файла с помощью Томита-парсера.
for filename in os.listdir(pathOutText):
    if filename.endswith(".txt"):
        try:
            try:
                os.remove(pathTomitaParser + "text.txt")
                os.remove(pathFactsXMLTomita + 'facts.xml')
            except:
                print('')
            textOut = open(pathOutText + filename, "r", encoding='utf-8')
            data = textOut.readlines()
            textOut.close()

            textTomita = open(pathTomitaParser + 'text.txt', "w", encoding='utf-8')
            textTomita.writelines(data)
            textTomita.close()

            os.chdir(pathTomitaParser)
            result = subprocess.run(['tomita-parser', 'config.proto'])

            factsXml = open(pathFactsXMLTomita + "facts.xml", 'r', encoding='utf8')
            data = factsXml.readlines()
            factsXml.close()

            mystr = re.sub(r"[.txt]", "", filename)
            factsOut = open(pathFacts + mystr + '.xml', 'w', encoding='utf8')
            factsOut.writelines(data)

            Сохранение фактов в виде html таблицы
            with open(pathDebug + 'debug.html', 'r') as read_file:
                 mystr = re.sub(r"[.txt]", "", filename)
                 html = open(pathOutDebug + "debug_" + mystr + ".html", "w")
                 i = 0
                 j = 0
                 while i == 0:
                    line = read_file.readline()
                    if line == "<table border=\"1\"><tbody>\n":
                        html.write(line)
                        i += 1
                        while j == 0:
                            line = read_file.readline()
                            if line != "<table border=\"1\"><tbody>\n":
                                html.write(line)
                            else:
                                j += 1
            
            html.close()
            read_file.close()
            factsOut.close()
            # os.remove(pathFactsXMLTomita + 'facts.xml')
        except:
            print("Error Tomita")

ResultAO = []
ResultAOC = []

# Запись в файл JSON и сохранение БД
for filename in os.listdir(pathFacts):
    if filename.endswith(".xml"):
        try:
            factsOutXml = open(pathFacts + filename, "r", encoding='utf-8')
            tree = ET.parse(factsOutXml)
            root = tree.getroot()
            mystr = re.sub(r"[.xml]", "", filename)
            for AOC in root.iter('AOC'):
                F = AOC.get('FactID')
                Act = AOC.find('Action').get('val')
                Obj = AOC.find('Object').get('val')
                try:
                    Con = AOC.find('Condit').get('val')
                except AttributeError:
                    ResultAO = Act + ' ' + Obj
                    update_document(Patents, {'Index': mystr}, {'AOC': {'Action': Act, 'Object': Obj}})

                    # print(F, Act, Obj)
                    continue
                ResultAOC = Act + ' ' + Obj + ' ' + Con
                # ResultAOC.append(Act + ' ' + Obj + ' ' + Con)

                update_document(Patents, {'Index': mystr}, {'AOC': {'Action': Act, 'Object': Obj, 'Condit': Con}})

            Patents.update_one({'Index': mystr}, {'$unset': {'Index': mystr}})
            # if root.iter
        except:
            print('Error DB')
