# -*- coding: utf-8 -*-
"""
Created on Wed May 12 12:20:11 2021

@author: Shivansh
"""
import requests
import time
import logging
import os

import pymongo
from pymongo import MongoClient

from telegram import *
from telegram.ext import *

from datetime import datetime
import pytz


mongoDB_API_key = os.getenv('MONGODB_API_KEY')
telegram_BOT_API_key = os.getenv('TELEGRAM_BOT_API_KEY')



logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)

##########<MongoDB Connection Code>##########

cluster = MongoClient(mongoDB_API_key,connect=False)
db = cluster["C19Notif"]
collectionUsers = db["users"]
collectionActiveLeads = db["activeleads"]

##########</MongoDB Connection Code>##########


##########<Statewise Data  Code>##########

allDistrictInfo = {}

statewiseData =  requests.get("https://cdn-api.co-vin.in/api/v2/admin/location/states",
                              headers = {"Accept" : '*/*' , "User-Agent" : "Mozilla/5.0"}).json()

for stateNo in range( 0, len(statewiseData['states'])):
    

    districtData = requests.get("https://cdn-api.co-vin.in/api/v2/admin/location/districts/" + str(statewiseData['states'][stateNo]['state_id']),
                            headers = {"Accept" : "*/*" , "User-Agent" : "Mozilla/5.0"}).json()
    
    stateName = statewiseData['states'][stateNo]['state_name']
    allDistrictInfo[stateName] = districtData['districts']
    

##########</Statewise Data  Code>##########
    
API_KEY = telegram_BOT_API_key
telegramBot = Bot(API_KEY)

updater = Updater( API_KEY , use_context = True )
dispatcher = updater.dispatcher
availibilityMessage = "Vaccines Available at : {}\nAddress : {}\nDose 1 availiblity : {}\nDose 2 availibility : {}\nCost : {}\nVaccine:{}\nMin age limit : {}"

def sendStartReply( update:Update, context:CallbackContext ):
    logging.info("Effective chat id for start command :" + str(update.effective_chat.id))
    telegramBot.send_message(update.effective_chat.id,"Hi please use the following commands to initiate the notifications for Slots in your region\n/getdistricts to get the information about the district numbers\n/registerjabs <district number> without <> to register for notifications for the vaccines",)

def registerJabs( update:Update , context:CallbackContext):
    logging.info("Register jabs command")
    chatId = str(update.effective_chat.id)
    
    if( context.args == [] ):
        telegramBot.send_message(update.effective_chat.id,"Please enter a valid district number",)
        return   
    
    if( int(context.args[0])<1 or int(context.args[0])>737):
        telegramBot.send_message(update.effective_chat.id,"Please enter a valid district number",)
        return
    
    document = { "chatId" : chatId , "districtNumber" : context.args[0]}
    
    logging.info("Register jabs command for: " + str(chatId) + " : District :" + str(context.args[0]))
    
    collectionUsers.insert_one(document)
    
    telegramBot.send_message(update.effective_chat.id,"Registered for notifications regarding vaccination slots in District "+ context.args[0] ,)    
    
    currentLeads = collectionActiveLeads.find( { "districtNumber" : context.args[0]} )
    
    if( currentLeads != {} ):
        for leads in currentLeads:
            logging.info("Sending info for active leads for newly registered: " + str(chatId) + " : District :" + str(context.args[0]))
            telegramBot.send_message(update.effective_chat.id,availibilityMessage.format(leads['name'],
                                                 leads['address'],
                                                 leads['available_capacity_dose1'],
                                                 leads['available_capacity_dose2'],
                                                 leads['fee_type'],
                                                 leads['vaccine'],
                                                 leads['min_age_limit']),)           
        
def getAllData( update:Update , context:CallbackContext):
    
    message = "District Numbers for districts in <b> {} </b> \n"
    logging.info("Get all district data for : " +  str(update.effective_chat.id))
    for state in allDistrictInfo.keys():
        sendMessage = message.format(state)
        
        for district in allDistrictInfo[state]:
            sendMessage = sendMessage + str(district["district_name"]) + ":" + str(district["district_id"]) +"\n"
        
        
        telegramBot.send_message( update.effective_chat.id , sendMessage , parse_mode=ParseMode.HTML,)
        
    
            
start_command = CommandHandler('start',sendStartReply)
register_command = CommandHandler('registerjabs', registerJabs , pass_args=True)
alldistricts_command = CommandHandler('getdistricts', getAllData , pass_args=True)

dispatcher.add_handler(start_command)
dispatcher.add_handler(register_command)
dispatcher.add_handler(alldistricts_command)

updater.start_polling()

##########</Telegram Bot Code>##########

while True:
    
    distinctDistrict = collectionUsers.distinct("districtNumber")
    
    
    for district in distinctDistrict:
        time.sleep(3)
        #print("FOR DISTRICT : " + district)
        
        IST = pytz.timezone('Asia/Kolkata')
        
        datetime_ist = datetime.now(IST)

        currentDate = datetime_ist.strftime("%d-%m-%y")
        
        response = requests.get("https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/findByDistrict?district_id="+district+"&date="+currentDate,
                 headers = {"Accept" : '*/*' , "User-Agent" : "Mozilla/5.0"})
        
        logging.info("Calling API for : " + district + ": current date " + currentDate)
    
        #Raigad District Number 339
        #Deharadun District Number 697
        responseJSON = response.json()
        
        if( len(responseJSON['sessions']) == 0 ):
            delQuery = {"districtNumber" : district}
            
            data = collectionActiveLeads.find( delQuery ) 
            
            if( len(list(data)) != 0  ):
                logging.info("Deleting all leads for : " + str(district) + " due to no availibilty")
                collectionActiveLeads.delete_many(delQuery)       
            
            
            
        else:
            
            checkQuery = {"districtNumber" : district}
            
            if( collectionActiveLeads.count_documents(checkQuery) == 0 ):
                
                logging.info("New leads for : " + str(district) )
                notifyChatIds = collectionUsers.find( { "districtNumber" : district } )
                
                
                for responseData in responseJSON['sessions']:
                    
                    responseData['districtNumber']=district
                    collectionActiveLeads.insert_one(responseData)
                    
                    for notif in notifyChatIds:
                        logging.info("SENDING MESSAGE TO : " + notif['chatId'] + " for new info ")
                        telegramBot.send_message(notif['chatId'],availibilityMessage.format(responseData['name'],
                                                 responseData['address'],
                                                 responseData['available_capacity_dose1'],
                                                 responseData['available_capacity_dose2'],
                                                 responseData['fee_type'],
                                                 responseData['vaccine'],
                                                 responseData['min_age_limit']),)
            else:
                oldLeads = collectionActiveLeads.find( {"districtNumber": district} )
                
                oldCenters = set()
                
                for x in oldLeads:
                    oldCenters.add(x['center_id'])
                    
                for responseData in responseJSON['sessions']:
                    if( responseData['center_id'] in oldCenters ):
                        #UPDATING OLD LEADS
                        collectionActiveLeads.delete_one( { "center_id" : responseData['center_id'] } )
                        responseData['districtNumber'] = district
                        collectionActiveLeads.insert_one(responseData)
                        oldCenters.remove(responseData['center_id'])
                    else:
                        #NEW LEAD
                        responseData['districtNumber'] = district
                        collectionActiveLeads.insert_one(responseData)
                        
                        notifyChatIds = collectionUsers.find( { "districtNumber" : district } )
                        
                        for notif in notifyChatIds:
                            logging.info("Sending leads which was not in active leads to : " + str(notif['chatId']) +" : with lead name " + responseData['name'])
                            telegramBot.send_message(notif['chatId'],availibilityMessage.format(responseData['name'],
                                                     responseData['address'],
                                                     responseData['available_capacity_dose1'],
                                                     responseData['available_capacity_dose2'],
                                                     responseData['fee_type'],
                                                     responseData['vaccine'],
                                                     responseData['min_age_limit']),)
                    
                for remaining in oldLeads:
                    #DELETING NULL LEADS
                    logging.info("Deleting deadlead")
                    collectionActiveLeads.delete_one( { "center_id" : remaining } )




    
    
    
    

    
    
        
        
                '''
                
                for old in oldLeads:
                    collectionActiveLeads.delete_one(old);
                    
                for responseData in responseJSON['sessions']:
                    responseData['districtNumber']=district
                    collectionActiveLeads.insert_one(responseData)
                
                
                
                
            
        
            for responseData in responseJSON['sessions']:
                checkQuery = {"districtNumber" : district}
                
                if(collectionActiveLeads.count( checkQuery ) == 0):
                    response
                    
                
                
                if( responseData['min_age_limit'] >=18  ):
                    print(responseData['name'])
                    
            '''