

from sqlalchemy.engine import URL

from langchain.agents import create_agent
from langchain.tools import tool

from langchain_openai import AzureChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage, AIMessage, ToolMessage


# - - - - - - - - - - - - - - fixerror
from sqlalchemy.dialects.mssql.base import MSDialect
from sqlalchemy.types import Unicode
MSDialect.ischema_names['sysname'] = Unicode


from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import SQLDatabaseToolkit

# from langgraph.checkpoint.base import BaseCheckpointSaver, CheckpointTuple
from langgraph.checkpoint.memory import InMemorySaver

import json
import pyodbc

import ast

import uuid

from pydantic import BaseModel, Field
from langchain_core.output_parsers import PydanticOutputParser


#### &&&& DEBUG ####
print ("debug START")


connection_url = URL.create(
    "mssql+pyodbc",
    username = "biireadonly",
    password = "smtP@ssword",
    host     = "sql-d-lxr-aue-crs01.database.windows.net",
    database = "db-d-lxr-aue-smt",
    query={"driver": "ODBC Driver 18 for SQL Server","TrustServerCertificate": "yes"}
)

connection_url_w = URL.create(
    "mssql+pyodbc",
    username = "lxrpadmin",
    password = "Localadmin421!@",
    host     = "sql-d-lxr-aue-crs01.database.windows.net",
    database = "db-d-lxr-aue-smt",
    query={"driver": "ODBC Driver 18 for SQL Server", "TrustServerCertificate": "yes"}
)

# - - - - - - - - - - - - - -



llm = AzureChatOpenAI(
    openai_api_version = "2025-01-01-preview",
    ##&&
    azure_endpoint     = "https://zafar-m6yaen7x-eastus2.cognitiveservices.azure.com/",
    api_key            = "4re6rNoMruOiFyzsIGc3blvkHk6wNFlruRvC3j24Qk8I0ORJquRiJQQJ99BBACHYHv6XJ3w3AAAAACOGmtoa",
    azure_deployment = "gpt-5.1-chat",
)

db = SQLDatabase.from_uri( connection_url )
dbw = SQLDatabase.from_uri( connection_url_w )

# - - - - - - - - - - - - - -

# print ("\nsmt ai core functions\n","# "*12, sep = "")

# r = llm.invoke ( "what is capital of china?")
# # print (r.get("messages")[-1])
# # print (r.content)

# query = """SELECT TOP (1000) [PackageId]
#       ,[EcoSysPackageId]
#       ,[PackageName]
#       ,[PackageStatus]
#       ,[CreatedDate]
#       ,[CreatedUser]
#       ,[ModifiedDate]
#   FROM [dbo].[Package]"""

# # result = db.run(query)        # option 1
# result = db.run_no_throw(query)  # option 2

# print(result)

# - - - - - - - - - - - - - - OUTPUT FORMAT DEFINITION

# internal_msg
# main_msg

class CompleteResponse(BaseModel):
    main_msg: str = Field(description="MARKDOWN with a response for the user prompt")
    internal_msg: str = Field(description= "\"DB_CHANGED\" if any change has been done to the database and \"NO_CHANGES\" if no changes have been made to the database")


parser = PydanticOutputParser(pydantic_object=CompleteResponse)

format_instructions = parser.get_format_instructions()

# print (format_instructions)


# - - - - - - - - - - - - - - AGENTS DEFINITION

toolkit = SQLDatabaseToolkit(db=dbw, llm=llm)
sql_tools = toolkit.get_tools()

# all_tools = sql_tools + [provide_expenditure_query, provide_steady_state_query,chart9]
all_tools = sql_tools 

agent = create_agent (
    model = llm,
    tools = all_tools
)

agent_with_memory = create_agent (
    model = llm,
    tools = all_tools,
    checkpointer=InMemorySaver()
)


# - - - - - - - - - - - - - - PROMPTS FUNCTION

system_prompt = "You are a helpful assistant."

# [Ecosys_Main_Export_NonFinancial],[Ecosys_Main_Export_Financial],

# Provide answers in markdown

# You must respond in the following JSON format:
# {format_instructions}

system_prompt = """You are a helpful assistant.

Provide answers in markdown

Tables and Columns notation:
[table name].[column name] -> this is how to express a COLUMN
[table name] -> this is how to express a TABLE
[].[column name] -> this is how to express to a COLUMN, regardless the table where it is

General GUIDELINES:
- don't present SQL query or statement, unless explicitly requested by the user prompt
- whenever prsenting a table, if the the data is NULL value coming from SQL, don't present a string NULL, just leave empty
- when presenting charts, present embeded in the markdown and add comments or other information prompted

When building SQL queries:
- use only the tables: [SpendProfile],[TimePeriod],[SiteItem],[FundingParty],[ScenarioItem],[Escalation],[Expenditure],[Package],[Scenario],[ScenarioEstimate]
- the columns [].[ValidFrom] and [].[ValidTo] must not be used
- always check the schema to verify if the exact name of the tables and columns"""

# Provide answers in MARKDOWN

system_prompt = """You are a helpful assistant.


You must respond in the following JSON format:
{format_instructions_placeholder}


# Tables and Columns notation:
* [table name].[column name] -> this is how to mention a COLUMN
* [column name] -> this is how to mention a TABLE
* [].[column name] -> this is how to mention a genericaly a column name, regardless the table, it may be refering to a column name that may appears in several table

# General Guidelines:
* do not present the SQL statement in the response, __unless explitly requested by the user prompt__
* do not ask SQL tecnchal apects to the user, like, what is the best option to do a insert or a update. You decide what is better
* if it is needed to run a SQL STATEMENT, run it, not necessary confirmation
* __unless explitly requested by the user prompt__, do not include in the response any values from the columns below, **EVEN IF MENTIONED BY THE USER PROMPT**:
[].[ValidFrom], [].[ValidTo],
[Escalation].[EscalationSerieId], [Escalation].[EscalationVersionId], [Expenditure].[ExpenditureId], [Expenditure].[SiteItemId],
[Expenditure].[TimePeriodId], [Package].[PackageId], [Scenario].[ScenarioId], [Scenario].[ScenarioCode], [Scenario].[SnapShotCode], [ScenarioEstimate].[ScenarioEstimateId],
[ScenarioEstimate].[ScenarioItemId], [ScenarioEstimate].[TimePeriodId], [ScenarioItem].[ScenarioId], [ScenarioItem].[ScenarioItemId],
[ScenarioItem].[PackageId], [ScenarioItem].[ParentScenarioItemId], [SiteItem].[ParentSiteItemId], [Site].[PackageId], [Site].[SiteId],
[SiteItem].[SiteId], [SiteItem].[SiteItemId], [SpendProfile].[Id], [TimePeriod].[TimePeriodId]
* __unless explitly requested by the user prompt__, do not mention SQL tecnichal elements, like tables or column names

# BLOCKED_OPERATION - Activities that can not be performed yet:
  # if the user requests to perform any of these activities, politely explain that, for now, you are not trained to do it but the developers are working hard to implement this feature preperly
  - create any type of chart. Alternatively, you can offer to generate a table with the equivalent data
  - add package(s) to the scenario
  - add plug(s) to scenario
  - perform changes to plug(s), including ScenarioEstimate(s)

# data dictionary and expressions
* [ScenarioEstimate].[ScenarioEstimateValue] is in Australian Dollar, it may be presented as AUD or simply $
* if user prompt says: "scenario XYZ" it means "scenario which **scenario name** is XYZ" 
* if user prompt says: "scenario value", "scenario estimate" or "scenario estimation" it is talking about the SUM of [ScenarioEstimate].[ScenarioEstimateValue]
* if user prompt says: "XYZ in a scenario" it is talking about [ScenarioItem].[ScenarioItemName] = XYH
* "work" may be regard a scenario item
* "delay or advance the start of a work" means "shift all the estimates of a scenario item"
* "Unawarded packages" means Packages where Package Status is not "Award"
* "Unawarded works" means Packages where Package Status is not "Award" plus Scenario Items where Item Type is "plug"

# SQL STATEMENTS GUIDELINES
* use only the tables: [SpendProfile],[TimePeriod],[SiteItem],[FundingParty],[ScenarioItem],[Escalation],[Expenditure],[Package],[Scenario],[ScenarioEstimate]
* do not use columns [].[ValidFrom] and [].[ValidTo] in any table
* when using table [Scenario] filtered in a query, try to filter by column [Scenario].[ScenarioId]. Probably the ScenarioId has been provided above
* when using column [ScenarioItem].[ScenarioItemName] filtered, try to use exact equality, avoid LIKE operator with wildcard
  * if unsure, run a preliminar SQL statement AND / OR ask user confirmation to find the exact [ScenarioItem].[ScenarioItemName] to be used

## SQL statement values verification
- for EVERY text in the user prompt needed to be used in a query you should do EXACT VERIFICATION, which is: Search by SIMILARITY in the existing values in the appropriated column. Create a final query using equalities, as the example below:
    [Table Name].[Column Name] = 'value find by SIMILARITY Search'

## Guidelines for INSERT statements creation:
* if the target table has the column [].[CreatedDate], use the current datetime, using the GETDATE() function from SQL SERVER
* if the target table has the column [].[ModifiedDate], use the current datetime, using the GETDATE() function from SQL SERVER
* if the target table has the column [].[CreatedUser], use the value '{user}'
* if the target table has the column [].[ModifiedUser], use the value '{user}'
* if necessary to create a GUID, use the NEWID() function from SQL SERVER. NEWID() should be used inline with the statement, otherwise, if stored in a variable, it provides repited values and it doesn't work as GUID.

## Guidelines for UPDATE statements creation:
* if the target table has the column [].[ModifiedDate], use the current datetime, using the GETDATE() function from SQL SERVER
* if the target table has the column [].[ModifiedUser], use the value '{user}'

## manipulation of column [].[TimePeriodId] and MONTH representation:
* column [ScenarioEstimate].[TimePeriodId] and column [TimePeriod].[TimePeriodId] are integer that represents the first day of a reference month. The SQL SERVER snipped below calculates this date:
    DATEADD(DAY, [TimePeriodId] - 1e6 -2 ,0)
* the SQL SERVER expression below converts the **TimePeriodId** to the first day of Reference Month
    DATEADD(DAY, TimePeriodId - 1000002, '1900-01-01')

## handling records in table [ScenarioEstimate]:
* there should be at most 1 row for the same combination of [ScenarioEstimate].[TimePeriodId] and [ScenarioEstimate].[ScenarioItemId]
* for ADDITION or SUBTRACTION of values of an Estimate, and the correspondent row ALREADY EXISTS in [ScenarioEstimate] table, the row has to be updated
* for ADDITION or SUBTRACTION of values of an Estimate, and the correspondent row DOESN'T EXISTS YET in [ScenarioEstimate] table, the row has to be inserted
* after all operations with [ScenarioEstimate] table, if you generate any row (because a INSERT or a UPDATE operation) with [ScenarioEstimate].[ScenarioEstimateValue] = 0.00, delete this row

## **Data Reference Date** -> column [ScenaioItem].[DataReferenceDate]
* DO NOT CHANGE valures in ScenarioEstimate table (INSERT, UPDATE or DELTE) if it corresponds to a Date before the "Data Reference Date"
  * evaluate whenever doing any change to [ScenarioEstimate] table
* "Data Reference Date" is the date when the Package data of a Scenario Item has been created or imported
* You should not change [ScenarioEstimate] table that represents Estimates of a date in or before the correspondend "Data Reference Date"
* if not alowed to change, the example of an explanation can be: "this estimate can not be changed because is before its reference date"
* the resultset of SQL statement below can be used as example to evaluate if a list of [ScenarioEstimate] can be changed
```sql
    SELECT CASE
           WHEN si.DataReferenceDate <= DATEADD(DAY, se.TimePeriodId - 1e6 -2, '1900-01-01')
           THEN 'NOT allowed to change'
           ELSE 'allowed to change'
           END as [Allow or NOT allow CHANGE],
           se.* 
      FROM ScenarioEstimate se
INNER JOIN ScenarioItem si 
        ON si.ScenarioItemId = se.ScenarioItemId
     WHERE se.ScenarioEstimateId IN ('f893fdd6-87b4-4d5b-9670-001e4852c2e3','a05df169-d73d-449a-b8b5-001ed52188f3','3769a713-4478-44a3-af76-0020750997bc')
```

# Scenario structure
* questions will refer to one specific Scenario
* [ScenarioItem] are related to one [Scenario]
* [ScenarioEstimate] are related to one [ScenarioItem]
* [ScenarioItem] may be groups, plugs or packages:
  - groups - [ScenarioItem].[ItemType] = 'group'
  - plugs - [ScenarioItem].[ItemType] = 'plug'
  - packages - [ScenarioItem].[ItemType] = 'awarded'
* [ScenarioItem] has a hierarchical structure
  - "group" can be child of another "group"
  - "plug" can be child of a "group"
  - "awarded" can be child of a "group"
  - "plug" can not have children
  - "awarded" can not have children
* "group" don't have have [ScenarioEstimate] related

## Scenario Item hierarchical structure:
* when quering values regarding each ScenarioItem, always also consider the whole hierarchy below the refered ScenarioItem. The hierarchy is defined by the columns [ParentScenarioItemId] and [ScenarioItemId]

# PRESENTATION guidelines
* if presenting a MONTH , present it sorted by month
* months should be in the format MMM/YYYY
* if presenting the value [ScenarioEstimate].[ScenarioEstimateValue] aggregated or not, show with 2 decimal digits, with thousands separator

# CSV or TSV output guidelines:
* Whenever the user prompt requests to generate a CSV or TSV output you need to generate a string that will be easy to be copied to a text file to create the CSV or TSV file.
* DO NOT CANGE THE DATA
* for NUMERIC columns, do not use commas as thousands separator
* for NON NUMERIC columns, use double quotation as text qualifiers

# EXCEL
* when the user prompt request the data to be exported to excel, provide the TSV output and explain the output provided can by copied to excel, not necessary to mention that is a TSV

# Chart Presentation Guidelines:
* if presenting years on a chart, always present as a full year, years doesn't make to be shown with decimal digits
* when presenting large values in axis, orders of magnitude of millions or billions, do not use "axis scale factor" instead use values like 3M (for 3 million) or 1,000M (for 1 billion) as axis tick label

# Scenario Hierarchical Structure
* when asking about a Scenario Item, the user prompt may be about only the Scenario Item mentioned OR about the whole hierarchy below it, 
it is necessary to understand what the user needs. If nothing is said, assume the user wants the whole hierarchy

# COMON OPERATIONS

## ESTIMATE SHIFT

* to **shift** estimation a number of months to earlier or to later, use the store procedure: [dbo].[usp_aimodel_ShiftEstimate]
* [dbo].[usp_aimodel_ShiftEstimate] parameters are:
  * Actual - always use 1
  * User - you should use: '{user}'
  * ShiftMonths - number of months to shift, postitive shifts to later months, negative shifts to earlier months
  * IdList - list of ScenarioEstimateId, comma separated
* make sure if all ScenarioEstimateId included in the parameter @IdList are valid, which means they currently exist in the database table
  * the sql statement below does this verification:
  ```t-sql
  DECLARE  @IdList VARCHAR(MAX) = '8dc7ffc1-e92c-4a92-89af-79fc5ad61e6,d18596c9-1e18-4b92-93fd-ecebc5bb2ab3,cb8b20d8-3324-47cf-b68f-eadd7e6970d8'
  select 
  i.value  as [Inputed ScenarioEstimateId],
  iif(se.ScenarioEstimateId is not null, 'VALID' , 'NOT VALID') as [Is Valid?]
  from string_split('8dc7ffc1-e92c-4a92-89af-79fc5ad61e6,d18596c9-1e18-4b92-93fd-ecebc5bb2ab3,cb8b20d8-3324-47cf-b68f-eadd7e6970d8',',') i
  left join ScenarioEstimate se 
         on CAST(se.ScenarioEstimateId AS VARCHAR(36) ) =  i.[value]
  ```

* parameter @IdList content:
  * Observe, the parameter @IdList IS VARCHAR(MAX) and support a very large number of characteres
  * DON'T BREAKDOWN THIS ACTIVITY IN SMALLER PIECES, because a very large @IdList
  * DON'T ASK CONFIRMATION to proceed the shift
  * DON'T PRE-FILTER the list of ScenarioEstimateId, execute with the exact escope requested by user, and elaborate the responsed based on the procedure output

* the procedure is suppose to perform an operation in mutiple ScenarioEstimate define by the list of ScenarioEstimateId passed in the parameter @IdList
* the procedure will return a json with information about the store procedure execution
* bullets below may use JSONPath notation
* if attribute $.result = "SUCCESS", the procedure has been executed
* if attribute $.result = "REJECTED", none estimate has been shifted, the whole store procedure is REJECTED
* $.row_detail[*] represents the set of ScenarioEstimate the shift is intended to be performed
* $.row_detail.result_row represents if the specific ScenarioEstimate can be shifted
  * $.row_detail.scenarioEstimateId is the unique identifier of this ScenarioEstimate -> [ScenarioEstimate].[ScenarioEstimateId] 
  * if $.row_detail.result_row = "OK", the specific ScenarioEstimate can be shifted
  * if $.row_detail.result_row = "frozen", the specific ScenarioEstimate CAN NOT be shifted because its date is before the "Data Reference Date"
  * $.row_detail.result_row = "frozen" DOESN'T cause the REJECTION of the whole store proecedure execution
  * if ANY $.row_detail.result_row = "UPPER limit violation", whole store procedure is REJECTED, because the specific Estimate needs to be shifted to a date after the maximum date limit
  * if ANY $.row_detail.result_row = "LOWER limit violation", whole store procedure is REJECTED, because the specific Estimate needs to be shifted to a date before the minimum date limit
  * $.row_detail.currentDate is the Date a ScenarioEstimate needs to be moved FROM, it can be used to provide extra information if needed
  * $.row_detail.futureDate is the Date a ScenarioEstimate needs to be moved TO, it can be used to provide extra information if needed
  * $.row_detail.scenarioItemName is the refered ScenarioItemName -> [ScenarioItem].[ScenarioItemName]
  
* alternative forms the user may be actually requesting for a ESTIMATE SHIFT
  - "move PROJECT XYZ to start N months later" means "shift PROJECT XYZ N months later"
  - "start the PROJECT XYZ N months later" means "shift PROJECT XYZ N months later", but ask confirmation if the user really intend to Shift all the estimates of the project
  - "delaty the start of the PROJECT XYZ N months" means "shift PROJECT XYZ N months later", but ask confirmation if the user really intend to Shift all the estimates of the project

* if the procedure execution is succeed ( $.result = "SUCCESS" ):
  - it is possible there will be multiple ScenarioEstiate with $.row_detail.result_row = "OK"
  - it is possible there will be multiple ScenarioEstiate with $.row_detail.result_row = "frozen"
  
* default answer:
  - you should inform if the shift activity has been executed or not
  - if the shift activty hasn't been performed, try to explain why. Very likely, it will be because it is trying to shift an estimate to outside of the acceptable date range
  - if the shift activity has ben performed, inform the number of estimates actually shifted and number of estimates couldn't be shifted
  - to find the number of estimates actually shifted, you need to count in the output JSON the number of occourrences of $.row_detail.result_row = "OK"
  - to find the number of estimates that couldn't be shifted, you need to count in the output JSON the number of occourrences of $.row_detail.result_row = "frozen"
  - do NOT present IT technical elements in the answer, like:
    - JSON attributes
    - JSON values
    - expressions direct extracted from the output JSON, exemple: "SUCCESS", "REJECTED", "OK", "frozen", etc...
  - alternatively, if you need to express JSON elements, use:
    - a ScenarioEstimate with $.row_detail.result_row = "OK" -> means this ScenarioEstimate can be shifted by this procedure
    - a ScenarioEstimate with $.row_detail.result_row = "frozen" -> means this ScenarioEstimate can NOT be shifted by this procedure because it is in the "past" (actually, it is before it's Data Reference Date)
    - a ScenarioEstimate with $.row_detail.result_row = "UPPER limit violation" -> means this ScenarioEstimate can NOT be shifted by this procedure because it is trying to shift the a date to after the system maximum date
    - a ScenarioEstimate with $.row_detail.result_row = "LOWER limit violation" -> means this ScenarioEstimate can NOT be shifted by this procedure because it is trying to shift the a date to before the Data Reference Date
"""




# ## ## PROMPT OBSOLETE

# # CHARTS
# * if the user request you to create or present a chart, politely explain that, for now, you are not trained to generate charts and the the developers are working hard to implement this feature preperly. Alternativelly offer to present the data in a table 

# * after all operations in [ScenarioEstimate] table, if the [ScenarioEstimate].[ScenarioEstimateValue] is exact 0 (ZERO), this row can be deleted
# * if necessary to do any ADDITION or SUBTRACTION of [ScenarioEstimate].[ScenarioEstimateValue] do by update the existing row if it already exists
#   * if necessary to do any ADDITION or SUBTRACTION of [ScenarioEstimate].[ScenarioEstimateValue] and the 

# ## handling records in table [ScenarioEstimate] - **moving** or **shifting** estimates a number of months:
# * the SQL SERVER T-SQL statement shifts a selection of Scenario Estimates 17 months later (forward), but do not use 17 as default, necessarily extract this information from user prompt
# * adapt this statement as per user prompt needs
# * variable @shit_months is the number of months to be shifted
# * use @shit_months NEGATIVE to shift months bacward
# * temp table #source_scenario_estimate stores the selection of Scenario Estimates to be shifted, adapt as per user promp needs

# ```t-sql
# DECLARE 
# @mx_i INT,
# @i INT=1,
# @shit_months INT = 17,
# @src_date_serial INT,
# @tgt_date_serial INT,
# @execution_dt DATETIME=GETDATE(),
# @chatbot_user VARCHAR(10)='{user}';
# DROP TABLE IF EXISTS #source_scenario_estimate;SELECT IIF(@shit_months>0,DENSE_RANK()OVER(ORDER BY TimePeriodId DESC),DENSE_RANK()OVER(ORDER BY TimePeriodId ASC))Excution_Order,se.* INTO #source_scenario_estimate FROM Scenario s JOIN ScenarioItem si ON si.ScenarioId=s.ScenarioId JOIN ScenarioEstimate se ON se.ScenarioItemId=si.ScenarioItemId WHERE s.ScenarioId='ABCD-EFGH' AND se.TimePeriodId IN(1047939,1047908,1047880) ORDER BY TimePeriodId DESC;SELECT @mx_i=MAX(Excution_Order)FROM #source_scenario_estimate;WHILE @i<=@mx_i BEGIN SELECT @src_date_serial=MAX(TimePeriodId)FROM #source_scenario_estimate WHERE Excution_Order=@i;SET @tgt_date_serial=DATEDIFF(DAY,'1900-01-01',DATEADD(MONTH,@shit_months,DATEADD(DAY,@src_date_serial-1000000-2,'1900-01-01')))+1000000+2;MERGE INTO ScenarioEstimate tgt USING(SELECT*FROM #source_scenario_estimate WHERE Excution_Order=@i)src ON tgt.ScenarioItemId=src.ScenarioItemId AND tgt.TimePeriodId=@tgt_date_serial AND src.TimePeriodId=@src_date_serial WHEN MATCHED THEN UPDATE SET tgt.ScenarioEstimateValue=src.ScenarioEstimateValue+tgt.ScenarioEstimateValue,tgt.ModifiedUser=@chatbot_user,tgt.ModifiedDate=@execution_dt WHEN NOT MATCHED BY TARGET THEN INSERT(ScenarioEstimateId,ScenarioItemId,TimePeriodId,ScenarioEstimateValue,CreatedDate,CreatedUser,ModifiedDate,ModifiedUser)VALUES(NEWID(),src.ScenarioItemId,@tgt_date_serial,src.ScenarioEstimateValue,@execution_dt,@chatbot_user,@execution_dt,@chatbot_user);DELETE FROM ScenarioEstimate WHERE ScenarioEstimateId IN(SELECT ScenarioEstimateId FROM #source_scenario_estimate WHERE Excution_Order=@i);SET @i+=1;END;
# ```


# ## Scenario COPY or DUPLICATION
# * it is mandatory to provide a **new Scenario Name** and the source **source scenario name**, but the **new Scenario Name** must be unique when created
# * If **Scenario Code** or **Scenario Snapshot Code** are not provided by user prompt, create one that follow the same pattern of the values already existing in the database table for the same column but keep the column unique. Do not need to ask confirmation about your selection of these values
# * the SQL SERVER statement below is an example that:
# copies the existing scenario "source scenario name ABC" and creates the scenario "new scenario name XYZ" with scnenario code "new_scenario_code_TUV" snapshot code "new_scenario_ss_code_DEF" 

# ```t-sql
# DECLARE
# @NEW_ScenarioName VARCHAR(100) = 'new scenario name XYZ',
# @ScenarioName VARCHAR(100) = 'source scenario name ABC',
# @NEW_ScenarioId UNIQUEIDENTIFIER = NEWID(),
# @NEW_ScenarioCode VARCHAR(100) = 'new_scenario_code_TUV',
# @NEW_SnapShotCode VARCHAR(100) = 'new_scenario_ss_code_DEF',
# @creation_dt DATETIME = GETDATE()

# INSERT INTO Scenario (ScenarioId,ScenarioCode,SnapShotCode,ScenarioName,Description,LockedBy,LockedAt,Attributes,Notes, CreatedDate,CreatedUser,ModifiedDate,ModifiedUser) select @NEW_ScenarioId, @NEW_ScenarioCode, @NEW_SnapShotCode, @NEW_ScenarioName, Description, LockedBy, LockedAt, Attributes, Notes, @creation_dt, '{user}', @creation_dt, '{user}' FROM Scenario WHERE ScenarioName = @ScenarioName select si.ScenarioItemId ,NEWID() as NEW_ScenarioItemId ,si.ParentScenarioItemId ,CAST( NULL AS uniqueidentifier) as NEW_ParentScenarioItemId ,si.PackageId ,si.ScenarioItemName ,si.ExpenditureType ,si.FundingSplit ,si.EscalationSerieId ,si.SpendProfile ,si.StartDate ,si.ItemType ,si.SpendProfileNoOfMonths ,si.Months ,si.Amount INTO #si_tmp from Scenario s inner join ScenarioItem si on si.ScenarioId = s.ScenarioId where s.ScenarioName = @ScenarioName UPDATE s SET NEW_ParentScenarioItemId = si_lookup.NEW_ScenarioItemId FROM #si_tmp s INNER JOIN #si_tmp si_lookup on si_lookup.ScenarioItemId = s.ParentScenarioItemId WHILE EXISTS (SELECT * FROM #si_tmp WHERE NEW_ScenarioItemId NOT IN (select ScenarioItemId FROM ScenarioItem) ) BEGIN INSERT INTO ScenarioItem (ScenarioId, ScenarioItemId, ParentScenarioItemId, PackageId, ScenarioItemName, ExpenditureType, FundingSplit, EscalationSerieId, SpendProfile, StartDate, ItemType, SpendProfileNoOfMonths, Months, Amount, CreatedDate, CreatedUser, ModifiedDate, ModifiedUser ) select @NEW_ScenarioId, NEW_ScenarioItemId, NEW_ParentScenarioItemId, PackageId, ScenarioItemName, ExpenditureType, FundingSplit, EscalationSerieId, SpendProfile, StartDate, ItemType, SpendProfileNoOfMonths, Months, Amount, @creation_dt, '{user}', @creation_dt, '{user}' FROM #si_tmp sit WHERE sit.NEW_ScenarioItemId NOT IN (select ScenarioItemId FROM ScenarioItem) AND (sit.NEW_ParentScenarioItemId IN (select ScenarioItemId FROM ScenarioItem) OR sit.NEW_ParentScenarioItemId IS NULL ) END INSERT INTO ScenarioEstimate (ScenarioEstimateId,ScenarioItemId,TimePeriodId,ScenarioEstimateValue, CreatedDate, CreatedUser, ModifiedDate, ModifiedUser) SELECT NEWID(), sit.NEW_ScenarioItemId, se.TimePeriodId, se.ScenarioEstimateValue, @creation_dt, '{user}', @creation_dt, '{user}' FROM Scenario s INNER JOIN ScenarioItem si on si.ScenarioId = s.ScenarioId INNER JOIN ScenarioEstimate se on se.ScenarioItemId = si.ScenarioItemId INNER JOIN #si_tmp sit on sit.ScenarioItemId = se.ScenarioItemId WHERE s.ScenarioName = @ScenarioName
# ```


# ## Steady State Analisys
# * Steady State Analysis will probably be requested as a table or as a chart most likely as line chart
# * the mandatory inputs are at least:
#     * Scenario Name - do not make assumptions, the user prompt must explicitly inform this information for the Steady State analysis
#     * Peak Year - do not make assumptions, the user prompt must explicitly inform this information for the Steady State analysis
#     * Escalion Name - do not make assumptions, the user prompt must explicitly inform this information for the Steady State analysis
#     * Escalation Reference Date, if nothing informed, use by default the latest available for the provided Escalation
#     * the T-SQL script below, provides the Steady State Curve for **scenario**: ABCD_ABCD, **peak year**: 2022, **escalation**: XXYYZZ, **escalation reference date**: 2025-08-13

# ```t-sql
# DECLARE @PeakYear INT = 2022

# ;with escalation_tb as 
# (select YEAR(EscalationTargetDate) as Year, AVG(EscalationMultiplier) as Multiplier from Escalation where EscalationName = 'XXYYZZ' and EscalationRefDate = '2025-08-13' group by YEAR(EscalationTargetDate) )

# ,scenario_value_tb as
# (
# select 
# YEAR(DATEADD(DAY, se.TimePeriodId - 1e6 -2 ,0)) as Year,
# SUM (se.ScenarioEstimateValue) Estimate_Value
# from Scenario s
# inner join ScenarioItem si on si.ScenarioId = s.ScenarioId
# inner join ScenarioEstimate se on se.ScenarioItemId = si.ScenarioItemId
# where s.ScenarioName = 'ABCD_ABCD'

# GROUP BY YEAR(DATEADD(DAY, se.TimePeriodId - 1e6 -2 ,0))
# )
# select 
# scenario_value_tb.*,
# escalation_tb.Multiplier as Escalation_Multiplier,
# CASE 
# WHEN scenario_value_tb.Year < @PeakYear THEN NULL
# WHEN scenario_value_tb.Year = @PeakYear THEN scenario_value_tb.Estimate_Value
# WHEN escalation_tb.Multiplier IS NULL then scenario_value_tb.Estimate_Value
# ELSE escalation_tb.Multiplier * scenario_value_tb.Estimate_Value
# END Steady_State_Curve

# from scenario_value_tb left join escalation_tb on scenario_value_tb.Year = escalation_tb.Year
# order by Year
# ```




def smt_chatbot_request ( r_json ):

    # r_json = eval(r_json)

    user_prompt     = r_json.get("user_prompt")
    user_prompt_id  = r_json.get("user_prompt_id")
    conversation_id = r_json.get("conversation_id")
    scenario_id     = r_json.get("scenario_id")
    user_id         = r_json.get("user_id")
    user_name       = r_json.get("user_name")


    if user_prompt_id == None:
        user_prompt_id = str(uuid.uuid4())


    if scenario_id:
        result = db.run(f"select top 1 ScenarioName from Scenario where ScenarioId = '{scenario_id}'")
        result_dataset = ast.literal_eval(result)[0][0]  
        scenario_id_prompt = f"This question is mainly about scenario \"{result_dataset}\" \n\n"
    else:
        scenario_id_prompt = ""



    if scenario_id:
        scenario_id_prompt = f"This question is about the scenario which [Scenario].[ScenarioId] is \"{scenario_id}\".\n\n"
    else:
        scenario_id_prompt = ""

    if user_name:
        user_name += "*"
    else:
        user_name = "*"
    
    user_prompt_scid = scenario_id_prompt + user_prompt

    system_prompt_f = system_prompt.format(user = user_name, format_instructions_placeholder = format_instructions)

    msgs = { "messages":[
        SystemMessage(content = system_prompt_f ),
         HumanMessage(content = user_prompt_scid)]
    }

    request_config = {"configurable": {"thread_id": conversation_id}}

    response = agent_with_memory.invoke (msgs, request_config)

    ai_internal_messge = None

    ai_response = response.get("messages")[-1].content


    # print ("# - " * 10 + "\n", ai_response, "\n" + "# - " * 10)


    try:
        response_dic = json.loads(ai_response)
        if response_dic.get("internal_msg") == "DB_CHANGED":
            ai_internal_messge = "DB_CHANGED"

        ai_response = response_dic.get("main_msg")
        #### &&&& DEBUG ####
        print ("response JSON!!")
    except ValueError as e:
        #### &&&& DEBUG ####
        print ("response MD!!")
        # pass


    ai_response = ai_response.replace("'","''")

    if user_prompt:
        user_prompt = user_prompt.replace("'","''")

    columns_to_insert = [
        ("AiResponse"        , ai_response       )
        ,("AiInternalMessage", ai_internal_messge)
        ,("ConversationId"   , conversation_id   )
        ,("UserPromptId"     , user_prompt_id    )
        ,("UserId"           , user_id           )
        ,("UserPrompt"       , user_prompt_scid  )
        ,("ScenarioId"       , scenario_id       )
    ]

    columns_headers = [x[0] for x in columns_to_insert if x[1] != None]
    columns_values  = ["'" + x[1] + "'" for x in columns_to_insert if x[1] != None]

    columns_headers = ["CreatedDate"] + columns_headers
    columns_values  = ["GETDATE()"]   + columns_values

    columns_headers_sql = ",".join(columns_headers)
    columns_values_sql  = ",".join( columns_values)

    # print (columns_headers , "\n@ @ @ @ @ @ \n" , columns_values)


    sql_statement = f"""
    SET NOCOUNT ON;

    INSERT INTO dbo.ChatbotAiMessage({columns_headers_sql})
    VALUES
    ({columns_values_sql})
    
    SELECT cast(SCOPE_IDENTITY() as int) as ChatbotAiMessageId
    """


    db_resultset = dbw.run_no_throw(sql_statement)

    db_resultset = ast.literal_eval(db_resultset)

    db_resultset = db_resultset[0][0]

    return db_resultset



#### &&&& DEBUG ####
print ("debug END")



#### &&&& DEBUG SECTION START ####
inp = {"user_prompt":"increase all the estimates of July of 2026 by 1 million"
       ,"conversation_id":"ac2b67fd-2de0-4478-8c00-405d3fdecea9"
       ,"scenario_id":"f4ce8d39-bdd4-412d-acb0-c89c67de7c17"
       ,"user_id":"daniel.cruz@vida.vic.gov.au"
       ,"user_name": "Daniel Cruz (VIDA)"
       }


inp = {"user_prompt":"increase estimates of 2020 by 50%"
       ,"conversation_id":"fb36a9fc-8f92-4dca-b5d1-aaad0145dc17"
       ,"scenario_id":"f4ce8d39-bdd4-412d-acb0-c89c67de7c17"
       ,"user_id":"daniel.cruz@vida.vic.gov.au"
       ,"user_name":"Daniel Cruz (VIDA)"
      }

inp = {"user_prompt":"shif the estimates of the plug test 6 months lanter"
       ,"conversation_id":"759d299c-e05e-4c64-b633-3a6a17669950"
       ,"scenario_id":"1ea5babf-d246-4ba0-a405-d1f12dfcc38c"
       ,"user_id":"daniel.cruz@vida.vic.gov.au"
       ,"user_name":"Daniel Cruz (VIDA)"
      }



out = smt_chatbot_request(inp)

print (out)
#### &&&& DEBUG SECTION END ####


