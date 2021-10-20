library(redCohort)
renv::restore()

# Maximum number of cores to be used:
maxCores <- parallel::detectCores()

# The folder where the study intermediate and result files will be written:
outputFolder <- ""

# Details for connecting to the server:
connectionDetails <-
        DatabaseConnector::createConnectionDetails(
                dbms = "",
                server = "",
                user = "",
                password = "",
                port = ""
        )

# The name of the database schema where the CDM data can be found:
cdmDatabaseSchema <- ""

# The name of the database schema and table where the study-specific cohorts will be instantiated:
cohortDatabaseSchema <- ""
cohortTable <- ""

# Some meta-information that will be used by the export function:
databaseId <- "REDCDM"
databaseName <- "REDCDM"
databaseDescription <- "REDCDM"

# For Oracle: define a schema that can be used to emulate temp tables:
tempEmulationSchema <- NULL

redCohort::execute(
        connectionDetails = connectionDetails,
        cdmDatabaseSchema = cdmDatabaseSchema,
        cohortDatabaseSchema = cohortDatabaseSchema,
        cohortTable = cohortTable,
        tempEmulationSchema = tempEmulationSchema,
        outputFolder = outputFolder,
        databaseId = databaseId,
        databaseName = databaseName,
        databaseDescription = databaseDescription,
        minCellCount = 1
        
)


CohortDiagnostics::preMergeDiagnosticsFiles(dataFolder = outputFolder)
#shared file: c:/REDCDM/diagnosticsExport/Results_REDCDM.zip

CohortDiagnostics::launchDiagnosticsExplorer(dataFolder = outputFolder)

conn <- DatabaseConnector::connect(connectionDetails)

sql <- SqlRender::render("SELECT b.cohort_definition_id,a.person_id,a.condition_concept_id,a.condition_start_date,b.cohort_start_date FROM @a.condition_occurrence as a right join (select cohort_definition_id,subject_id,cohort_start_date from @b.@c) as b on a.person_id = b.subject_id order by b.cohort_definition_id", a = cdmDatabaseSchema, b = cohortDatabaseSchema, c = cohortTable)

sql <- SqlRender::translate(sql, dbms)

df <- DatabaseConnector::querySql(conn,sql)

csvFolder = paste0(outputFolder,"/REDCDM_condition.csv")
write.csv(df,csvFolder)
DatabaseConnector::disconnect(conn)


