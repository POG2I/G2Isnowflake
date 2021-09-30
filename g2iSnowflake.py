#!/usr/bin/env python
# Copyright © 2020/2021 Get2Insight  (PO)

import logging
from logging.config import dictConfig
import tempfile
import argparse
import sys, os
import csv
import snowflake.connector
import pandas as pd
import pyodbc
import simplejson as simplejson
import json
from azure.storage.blob import BlobServiceClient

import snowflake.connector

global tempdir
tempdir='c:/sftemp/'

global tableNameCol
tableNameCol = 2
import os

def getPostprocessFiles(location):
    files = os.listdir(location)
    files.sort(reverse=False)
    procdict = { }
    for x in files:
        if x.startswith("Reporting.") and x.lower().endswith(".script"):
            procdict[x.removesuffix(".script")]=x
    return procdict

def parseScriptFiles(location, filelist):
    scriptcommands = []
    for x in filelist:
        with open(location+x+".script", "r") as f:
            content = f.readlines()
            scriptlines = ""
            for x in content:
                if x.strip()=="--GO;":
                    if len((scriptlines.strip())) != 0: scriptcommands.append(scriptlines)
                    scriptlines=""
                else:
                    if x.strip!="--GO;":scriptlines=scriptlines+x

            if len((scriptlines.strip()))!=0 and x.strip()!="--GO;": scriptcommands.append(scriptlines)
            i=0
    return(scriptcommands)

def GenerateStoredProc(processdir):
    dictfile = getPostprocessFiles(processdir)

    settingsfile=next((f for f in dictfile if f.endswith("._Settings")), None)
    variablefile=next((f for f in dictfile if f.endswith("._Variables")), None)

    if settingsfile==None or variablefile==None:
        print("Settings or Variable file is missing, terminating.")
        quit()

    dimensions=[f for f in dictfile if ".uspCreateDim" in f]
    facts = [f for f in dictfile if ".uspCreateFact" in f]

    cmdlist = []

    for x in parseScriptFiles(processdir, [settingsfile]): cmdlist.append(x)
    for x in parseScriptFiles(processdir, [variablefile]): cmdlist.append(x)
    for x in parseScriptFiles(processdir, dimensions): cmdlist.append(x)
    for x in parseScriptFiles(processdir, facts): cmdlist.append(x)

    print("Preparing execution of "+str(len(cmdlist))+" commands")

    snowflakediagstable = "var rs = snowflake.execute( { sqlText: `CREATE OR REPLACE TABLE ""Reporting"".Log AS SELECT GetDate() as timestamp, cast('Started' as nvarchar(500)) as Description` } );"

    snowflakecmd=""
    snowflakecmdprefix=   "" \
                    "CREATE or replace PROCEDURE Reporting.RunUpdate() " \
                    "RETURNS VARCHAR" \
                    " LANGUAGE javascript EXECUTE AS CALLER AS " \
                    " $$ "

    for cmd in cmdlist:
        diagline = cmd.splitlines()[0].replace('"','').replace('\'','').replace('CREATE OR REPLACE TABLE','').replace(' AS','').replace('create schema if not exists ','Creating schema ')
        if diagline.startswith('set var_'): diagline='Setting Config'
        snowflakecmd=snowflakecmd+ \
                     "var rs = snowflake.execute( { sqlText: `INSERT INTO ""REPORTING"".Log(timestamp, description) VALUES(getdate(), 'Running "+diagline+"')` } );" +\
                     "\n var rs = snowflake.execute( { sqlText: `"+cmd+"` } );"+ \
                     "var rs = snowflake.execute( { sqlText: `INSERT INTO ""REPORTING"".Log(timestamp, description) VALUES(getdate(), 'Completed " + diagline + "')` } );"

    snowflakecmdpostfix="\n\n return 'Successful';" \
                    "$$;"

    snowflakecmd=snowflakecmdprefix+snowflakediagstable+snowflakecmd+snowflakecmdpostfix

    return(snowflakecmd)

def ScriptPath():
    print('sys.argv[0] =', sys.argv[0])
    pathname = os.path.dirname(sys.argv[0])
    #print('path =', pathname)
    #print('full path =', os.path.abspath(pathname))
    return pathname

def buildSnowflakeSchematructures(tablelist):
    schemas = []
    schemacommands = []
    for tbl in tablelist:
        if (tbl[0] not in schemas):
            schemas.append(tbl[0])
            #print("New Schema:"+tbl[0])
    for sch in schemas:
        schemacreate = 'CREATE SCHEMA IF NOT EXISTS "' + sch + '"'
        schemacommands.append(schemacreate)
    return schemacommands

def buildSnowflakeTableStructures(tablelist,cnxn):
    tablecommands = []

    for tbl in tablelist:
            if tbl[3]=="SQL":
                sqlq=tbl[1]
            else:
                sqlq="select * from "+tbl[1]+" WITH(NOLOCK)"

            querysql ="sp_describe_first_result_set N'"+sqlq+"'"
            querystruct = pd.read_sql_query(querysql,cnxn)
            cq = ""
            sqf = ""
            for rd in querystruct.values:
                vartype=rd[5]
                if vartype=="varbinary(max)":
                        vartype="varbinary(8388608)"
                elif vartype=="nvarchar(max)":
                    vartype = "nvarchar(8388608)"
                elif vartype=="varchar(max)":
                    vartype = "varchar(8388608)"
                elif vartype.lower()=="uniqueidentifier":
                    vartype= "varchar(50)"
                cq=cq+sqf+ ' "' +rd[2] + '" ' + vartype + ' NULL '
                sqf = ","
            cq='CREATE OR REPLACE TABLE "'+tbl[0]+'".'+tbl[tableNameCol]+' ( ' + cq + '  ) '
            tablecommands.append(cq)
    return tablecommands

def snowflakePandasCsvFormat(tablelist):
    formatlist = []
    formatdefi = "Create or replace FILE FORMAT CSV_DQ COMPRESSION = 'gzip'  FIELD_DELIMITER = ',' SKIP_HEADER=1  FIELD_OPTIONALLY_ENCLOSED_BY='\"' NULL_IF = ('NULL','null','') BINARY_FORMAT = BASE64;"
    formatlist.append(formatdefi)
    schemas = []
    schemacommands = []
    for tbl in tablelist:
        if (tbl[0] not in schemas):
            schemas.append(tbl[0])
            #print("New Schema:" + tbl[0])
    #for sch in schemas:
        #schemacreate = 'alter stage "' + sch + '".csv_dq set file_format = (format_name = ''"public".csv_dq''); '
        #formatlist.append(sch)
    return formatlist

def buildSnowflakeTableInsertStructure(tablelist,cnxn):
    loadcommands = []
    for tbl in tablelist:
            if tbl[3]=="SQL":
                sqlq=tbl[1]
            else:
                sqlq="select * from "+tbl[1]+" WITH(NOLOCK)"

            querysql    ="sp_describe_first_result_set N'"+sqlq+"'"
            querystruct = pd.read_sql_query(querysql,cnxn)
            cq = ""
            sqf = ""
            csvq = ""
            columnid=1 # We will start loading data from the second column (the first is the column from the pandas)
            trunkcmd='truncate table if exists \"'+tbl[0]+'\".\"'+tbl[2]+'\";'
            loadcommands.append([tbl[0]+'.'+tbl[2],trunkcmd])
            for rd in querystruct.values:
                columnid=columnid+1
                cq=cq+sqf+ ' "' +rd[2] + '"'
                csvq=csvq+sqf+"T.$"+str(columnid)
                sqf = ","
            cq='INSERT INTO "'+tbl[0]+'".'+tbl[tableNameCol]+' ('+cq+') SELECT '+csvq+' FROM @'+appConfiguration["snowflakeSchema"]+'.az_g2iload/'+tbl[0]+'_'+tbl[2]+'.csv.gz (FILE_FORMAT => ''csv_dq'') as T;'
            loadcommands.append([tbl[0]+'.'+tbl[2],cq])
    #print(loadcommands)
    return loadcommands

def validateTableList(tableList):
    print("Validating table list ...")
    tableDict = {}
    stopexec=False
    for x in tableList:
        tbl=x[2]
        if  tbl not in tableDict:
            tableDict[tbl]="table"
        else:
            print("Duplicate destination table:"+tbl)
            stopexec=True
    if stopexec:
            print("Process terminated")
            quit()

def readConfigTableList(location):
    data = None
    with open(location,"r") as f:
        data = json.load(f)
        return data

def updateConfiguration(location):
    jsonconfig = {
                    "version" : "8",
                    "blobStorageAccount":"<url>" ,
                    "blobStorageCredential": "<key>",
                    "blobStorageContainer": "igtest",
                    "blobStorageSASReadToken" : "?sv=2020-02-10&ss=bfqt&srt=sco&sp=rwdlacupx&se=2021-10-30T08:41:26Z&st=2021-04-12T00:41:26Z&spr=https&sig=3aZJeJN8CBTBaDneCMfmdq%2FFWLaAktAl%2BVeE3UQ7GmU%3D",
                    "SqlServer" : "localhost\\sql2019",
                    "SqlDatabase" : "ax2012ig",
                    "SqlAuth" : "trusted",
                    "SqlUser": "<user>",
                    "SqlPassword": "<password>",
                    "snowflakeUser" : "<username>",
                    "snowflakePassword": "<password>",
                    "snowflakeAccount" : "<snowflakeaccount>",
                    "snowflakeDatabase": "<databasename>",
                    "snowflakeSchema" : "PUBLIC",
                    "etlScriptLocation" : "<scriptlocation>",
                    "tempdir" : ""
    }

    with open(location, "r") as f:
        existingparams = simplejson.load(f)

    if ((existingparams==None or 'version' not in existingparams) or int(existingparams['version'])<int(jsonconfig['version'])):
        print("Updating configuration")
        if existingparams != None:
            for x in existingparams:
                if x in jsonconfig:
                    if x!="version":
                        #print("Keeping the key: "+x)
                        jsonconfig[x] = existingparams[x]
                else:
                    print("Adding new configuration entry: "+x)
        json=simplejson.dumps(jsonconfig, indent=4)
        with open(location, "w") as f:    f.write(json)
    with open(location, "r") as f:
        data = simplejson.load(f)
    return data

def snowflakeAzureStageSetup():
    snowAzure="CREATE OR REPLACE STAGE az_g2iload URL = 'azure://"+appConfiguration["blobStorageAccount"] + ".blob.core.windows.net/"+appConfiguration["blobStorageContainer"]+"' " \
              "CREDENTIALS = (azure_sas_token='"+appConfiguration["blobStorageSASReadToken"]+"')"
    return [snowAzure]

def snowflakeDiags():
    ctx = snowflake.connector.connect(
        user=appConfiguration['snowflakeUser'],
        password=appConfiguration['snowflakePassword'],
        account=appConfiguration['snowflakeAccount'],
    )
    cs = ctx.cursor()
    try:
        cs.execute("SELECT current_version()")
        one_row = cs.fetchone()
        print("Connected to Snowflake version "+one_row[0])
    finally:
        cs.close()
    ctx.close()

def snowflakeBatchExecute(commands,message = "Snowflake commands",schema="PUBLIC"):
    print('Starting execution: '+message, end="")
    ctx = snowflake.connector.connect(
        database=appConfiguration['snowflakeDatabase'],
        user=appConfiguration['snowflakeUser'],
        password=appConfiguration['snowflakePassword'],
        account=appConfiguration['snowflakeAccount'],
    )
    cs = ctx.cursor()
    if schema!="":
        schemacmd = "USE SCHEMA \""  + appConfiguration['snowflakeDatabase'] +"\".\"" + schema + "\""
        if schema.lower()!="public": print("\nChanged schema to: "+schema)
        #print(schemacmd)
        cs.execute(schemacmd)
    singleresult=False
    if len(commands)<2: singleresult=True
    try:
        for cmd in commands:
            #print(cmd)   # toggle on/off debugging
            cs.execute(cmd)
            one_row = cs.fetchone()
            if singleresult==True:
                    print(" ",end="")
            else:
                    print(" ")
            print(one_row[0], end="")
    finally:
        cs.close()
    ctx.close()
    print('... done')

def writeConfigTableList(conn,location):
    prefixdestination="b.name+'_'"
    #prefixdestination="''"
    with conn.cursor() as cursor:
        cursor.execute(
        "SELECT b.name as schemaname,a.name as tbName,"+prefixdestination+"+a.name tblName from sys.tables a left join sys.schemas b on a.schema_id=b.schema_id where a.name not like 'SYSOUTG%' and a.name not like 'DYNJ%';")
        row = cursor.fetchone()
        tablelist = []
        while row:
            fieldname='"'+row[0]+'"."'+row[1]+'"'
            tablelist.append([row[0],fieldname, row[2], 'FullTable'])
            row = cursor.fetchone()
        jsontablelist = simplejson.dumps(tablelist, indent=4, sort_keys=True)
        with open(location, "w") as f:    f.write(jsontablelist)
        return tablelist

def main():
    defaultschema="PUBLIC"

    #logging.basicConfig(filename='c:/temp/g2iSnowflake.log',
    #                    format='%(asctime)s %(levelname)-8s %(message)s',
    #                    datefmt='%Y-%m-%d %H:%M:%S',
    #                    encoding='utf-8',
    #                    level=logging.INFO)

    #logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

    print('GET2INSIGHT SQL to Snowflake Integration v1.10b')
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', help='config help')
    parser.add_argument('--etl', help='etl help')
    parser.add_argument('--temp', help='temp help')
    global args
    args = parser.parse_args()

    if args.config==None or args.etl==None:
        print('Required arguments must be supplied: --config, --etl')
        quit()

    if (not os.path.isfile(args.config)):
        print("The specified configuration file is missing: "+args.config)
        quit()

    if (not os.path.isfile(args.etl)):
        print("The specified ETL file is missing: "+args.etl)
        quit()

    fileConfiguration = args.config

    global appConfiguration
    appConfiguration=updateConfiguration(fileConfiguration)

    if appConfiguration['snowflakeSchema']!=None and appConfiguration['snowflakeSchema']!="":
        defaultschema=appConfiguration['snowflakeSchema']

    if appConfiguration['tempdir']!=None and appConfiguration['tempdir']!="":
        tempdir=appConfiguration['tempdir']
    else:
        if args.temp != None and args.temp != None:
            tempdir=args.temp

    schemas = []
    tablecommands = []
    schemacommands = []
    loadcommands = []

    snowflakeDiags()

    if str.lower(appConfiguration["SqlAuth"]) == "trusted":
        cnxn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + appConfiguration["SqlServer"] + ';DATABASE=' +
            appConfiguration["SqlDatabase"] + ';Trusted_Connection=yes;')
    elif str.lower(appConfiguration["SqlAuth"]) == "standard":
        cnxn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + appConfiguration["SqlServer"] + ';DATABASE=' +
            appConfiguration["SqlDatabase"] + ';UID=' + appConfiguration["SqlUser"] + ';PWD=' + appConfiguration[
                "SqlPassword"])
    else:
        print("SQL Authentication must be set to trusted or windows")

    tablelist = readConfigTableList(args.etl)

    validateTableList(tablelist)

    print('Connecting to blob service')

    blobService = BlobServiceClient(
        account_url="https://" + appConfiguration["blobStorageAccount"] + ".blob.core.windows.net/",
        credential=appConfiguration["blobStorageCredential"], container=appConfiguration["blobStorageContainer"])

    runetl = True
    if runetl==True:
        print("Processing tables")

        # retrieve all commands to execute on Snowflake platform
        formatcommands = snowflakePandasCsvFormat(tablelist)
        # print(formatcommands)
        snowflakeBatchExecute(formatcommands, "Snowflake Formats", defaultschema)

        stagecommands = snowflakeAzureStageSetup()
        snowflakeBatchExecute(stagecommands, "Snowflake Stages",defaultschema)

        schemacommands = buildSnowflakeSchematructures(tablelist)
        tablecommands = buildSnowflakeTableStructures(tablelist,cnxn)
        loadcommands = buildSnowflakeTableInsertStructure(tablelist,cnxn)

        snowflakeBatchExecute(schemacommands, "Snowflake Schemas",defaultschema)
        snowflakeBatchExecute(tablecommands, "Snowflake Tables", defaultschema)

        for tbl in tablelist:
            if tbl[3] == "SQL":
                sqlq = tbl[1]
            else:
                sqlq = "select * from " + tbl[1] + " WITH(NOLOCK)"

            print('Executing query for ' + tbl[2] + '... ', end='')
            csvdata = pd.read_sql_query(sqlq, cnxn)
            print('Read ' + tbl[2] + ' ' + str(len(csvdata.index)) + ' rows .... ')

            csvdata.to_csv(tempdir + tbl[0] + '_' + tbl[2] + '.csv.gz', compression='gzip',
                           quoting=csv.QUOTE_NONNUMERIC, encoding="utf-8")
            print('Wrote ' + tbl[2])

            with open(tempdir + tbl[0] + '_' + tbl[2] + '.csv.gz', "rb") as gzfile:
                print("Uploading...", end='')
                data = gzfile.read()
                container = blobService.get_blob_client(container=appConfiguration["blobStorageContainer"],
                                                        blob=tbl[0] + '_' + tbl[2] + '.csv.gz')
                container.upload_blob(data, overwrite=True)
                print(" done")

            for cmd in loadcommands:
                # print([tbl[2] + "/" + cmd[0]])
                tableid = tbl[0]+'.'+tbl[2]
                if cmd[0] == tableid:
                    lcmds = [cmd[1]]
                    snowflakeBatchExecute(lcmds, message="Loading data to table " + tbl[2], schema=defaultschema)
                    #snowflakeBatchExecute(lcmds, message="Loading data to table " + tbl[2], schema=tbl[0])

        print('Done running data preparation')

    etlscript=appConfiguration["etlScriptLocation"]
    spdata = GenerateStoredProc(etlscript)
    snowflakeBatchExecute([spdata],"Updating Stored Procedure",schema=defaultschema)
    snowflakeBatchExecute(["CALL Reporting.RunUpdate()"], "Executing Stored Procedure",schema=defaultschema)

#EntryPoint
main()
