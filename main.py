import logging
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
import pyspark.sql.functions as functions
from pyspark.sql.functions import lit
import os
######################################################
#   COMMONS CLASS
######################################################
class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

class Constants(metaclass=Singleton):

    usersQuerySQL = "select age , created_at, updated_at, document, document_type,email,family_type, first_name, fullname, vpc_desarrollo, vpc_fidelizacion from users where document is not null"

class CommonsUtils():

    @staticmethod
    def getLoggerModule(moduleName):
        logger = logging.getLogger(moduleName)
        return logger

    @staticmethod
    def loadInformationBackend(sql):
        # spark = SparkSessionInstance().spark
        spark = SparkSession.builder \
            .appName("PySpark Teradata Example") \
            .master("local") \
            .getOrCreate()
        driverPostgresql = api_key = os.environ['DRIVERPOSTGRES']
        url = api_key = os.environ['URLPOSTGRES']
        user = api_key = os.environ['USERPOSTGRES']
        password = api_key = os.environ['PASSWORDPOSTGRES']
        return spark.read \
            .format('jdbc') \
            .option('driver', driverPostgresql) \
            .option('url', url) \
            .option('dbtable', '({sql}) as src'.format(sql=sql)) \
            .option('user', user) \
            .option('password', password) \
            .load()


    @staticmethod
    def getCatalogFromCsv(pathGet, nameCatalog, dateBegin, dateEnd, idGroup, sepa=','):
        spark = SparkSessionInstance().spark

        date_cols = ["created_at"]

        catalogReadPD = pd.read_csv(pathGet, sep=sepa, parse_dates=date_cols, encoding="utf-8")

        catalogReadOnlyDelta = catalogReadPD[
            catalogReadPD.created_at.between(np.datetime64(dateBegin), np.datetime64(dateEnd))]
        catalogReadOnlyDelta.valor = catalogReadOnlyDelta.valor.astype(str)
        catalogReadDF = spark.createDataFrame(catalogReadOnlyDelta)
        catalogChangesColumns = catalogReadDF.select(
            catalogReadDF.id.alias("Caracteristica_Desc"),
            catalogReadDF.valor.alias("Codigo_Caracteristica_Op"),
            catalogReadDF.created_at.alias("Fecha_Creacion"))

        catalogOutPut = catalogChangesColumns \
            .withColumn("Grupo_Caracteristica_Id", lit(idGroup)) \
            .withColumn("Fecha_Modificacion", catalogChangesColumns.Fecha_Creacion)

        return catalogOutPut

class SparkSessionInstance(metaclass=Singleton):

    spark = None;

    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("PySpark Teradata Example") \
            .master("local") \
            .getOrCreate()

class MasterCatalogs(metaclass=Singleton):
    vPCPsicologicalResultsAndRiskLevelCatalog = None
    ageCatalog = None
    categoryOutcomeCatalog = None
    groupOutcomesCatalog = None
    subgroupOutcomeCatalog = None
    financialSourceCatalog = None
    eventOptionsCatalog = None
    worldsCatalog = None
    statesWorldsCatalog = None
    purposesCatalog = None
    prioritiesPsycologicalCatalog = None
    familyOptionCatalog = None
    financialHealthCatalog = None
    houseTypeCatalog = None
    houseStateCatalog = None
    nichosCatalog = None
    namesCatalogs = None
    versionHealthCatalog = None

    def __init__(self, dateBegin, dateEnd):
        self.vPCPsicologicalResultsAndRiskLevelCatalog = CommonsUtils.getCatalogFromCsv(
            'https://testsaludfinancieratera.s3.amazonaws.com/catalogs/Catalogo-VPC-ResultadosPsicologicos-NivelRiesgo.csv',
            'VPC-ResultadosPsicologicos-LevelRisk', dateBegin, dateEnd, idGroup=10)
        self.ageCatalog = CommonsUtils.getCatalogFromCsv(
            'https://testsaludfinancieratera.s3.amazonaws.com/catalogs/AgeCatalog.csv', 'Age', dateBegin, dateEnd,
            idGroup=16)
        self.financialSourceCatalog = CommonsUtils.getCatalogFromCsv(
            'https://testsaludfinancieratera.s3.amazonaws.com/catalogs/FinancialCatalog.csv', 'FinancialSource',
            dateBegin, dateEnd, idGroup=18)
        self.worldsCatalog = CommonsUtils.getCatalogFromCsv(
            'https://testsaludfinancieratera.s3.amazonaws.com/catalogs/WorldsCatalog.csv', 'Worlds', dateBegin, dateEnd,
            idGroup=9)
        self.statesWorldsCatalog = CommonsUtils.getCatalogFromCsv(
            'https://testsaludfinancieratera.s3.amazonaws.com/catalogs/StateWorldsCatalog.csv', 'StateWorlds',
            dateBegin, dateEnd, idGroup=22)
        self.familyOptionCatalog = CommonsUtils.getCatalogFromCsv(
            'https://testsaludfinancieratera.s3.amazonaws.com/catalogs/FamilyOptions.csv', 'FamilyOptions', dateBegin,
            dateEnd, idGroup=15)
        self.financialHealthCatalog = CommonsUtils.getCatalogFromCsv(
            'https://testsaludfinancieratera.s3.amazonaws.com/catalogs/FinancialHealthCatalog.csv', 'FinancialHealth',
            dateBegin, dateEnd, idGroup=19)
        self.versionHealthCatalog = CommonsUtils.getCatalogFromCsv(
            'https://testsaludfinancieratera.s3.amazonaws.com/catalogs/VersionFinancialHealthCatalog.csv',
            'VersionHealth', dateBegin, dateEnd, idGroup=20)
        self.houseTypeCatalog = CommonsUtils.getCatalogFromCsv(
            'https://testsaludfinancieratera.s3.amazonaws.com/catalogs/HouseType.csv', 'HouseType', dateBegin, dateEnd,
            idGroup=12)
        self.houseStateCatalog = CommonsUtils.getCatalogFromCsv(
            'https://testsaludfinancieratera.s3.amazonaws.com/catalogs/HouseState.csv', 'HouseState', dateBegin,
            dateEnd, idGroup=14)
        self.nichosCatalog = CommonsUtils.getCatalogFromCsv(
            'https://testsaludfinancieratera.s3.amazonaws.com/catalogs/NichosCatalog.csv', 'NichosCatalog', dateBegin,
            dateEnd, idGroup=21)
        self.namesCatalogs = CommonsUtils.getCatalogFromCsv(
            'https://testsaludfinancieratera.s3.amazonaws.com/catalogs/NamesCatalog.csv', 'NamesCatalog', dateBegin,
            dateEnd, idGroup=0, sepa='@')

        self.categoryOutcomeCatalog = None
        self.groupOutcomesCatalog = None
        self.subgroupOutcomeCatalog = None
        self.eventOptionsCatalog = None
        self.purposesCatalog = None
        self.prioritiesPsycologicalCatalog = None

######################################################
#   EXCEPTIONS CLASS
######################################################
class NotControledException(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(str(self.message))


######################################################
#   LAYOUTS CLASS
######################################################
class CatalogLayout:

    @staticmethod
    def execute(dateBegin, dateEnd):
        return 'OK'


class UserLayout:

    @staticmethod
    def execute(dateBegin, dateEnd):
        masterCatalogs = MasterCatalogs(dateBegin, dateEnd)

        vpcCatalogDF = masterCatalogs.vPCPsicologicalResultsAndRiskLevelCatalog
        vpcCatalogOnlyColumnsDF = vpcCatalogDF.select(vpcCatalogDF.Caracteristica_Desc,
                                                      vpcCatalogDF.Codigo_Caracteristica_Op)
        vpcCatalogOnlyColumnsDF.show()
        vpcCatalogDCIT = dict(vpcCatalogOnlyColumnsDF.collect())

        userSQL = Constants.usersQuerySQL
        usersReadDF = CommonsUtils.loadInformationBackend(userSQL)
        print('Lectura BD: ' + str(usersReadDF.count()))

        usersTransformOutputDF = usersReadDF.select(usersReadDF.age.alias("Cant_Edad"),
                                                    usersReadDF.created_at.alias("Fecha_Creacion")
                                                    , usersReadDF.updated_at.alias("Fecha_Modificacion"),
                                                    usersReadDF.document.alias("Numero_Identificacion")
                                                    , usersReadDF.document_type.alias("Tipo_Identificacion_Cd"),
                                                    usersReadDF.email.alias("Email_Txt")
                                                    , usersReadDF.family_type.alias("Tipo_Familia_Txt"),
                                                    usersReadDF.first_name.alias("Primer_Nombre_Txt"),
                                                    usersReadDF.fullname.alias("Nombre_Completo_Txt"),
                                                    usersReadDF.vpc_desarrollo.alias("Vpc_Desarrollo_Op")
                                                    , usersReadDF.vpc_fidelizacion.alias("Vpc_Fidelizacion_Op"))

        usersLowerCaseDF = usersTransformOutputDF \
            .withColumn("Vpc_Desarrollo_Op", functions.lower("Vpc_Desarrollo_Op")) \
            .withColumn("Vpc_Fidelizacion_Op", functions.lower("Vpc_Fidelizacion_Op"))


        usersCleanDF = usersLowerCaseDF \
            .na.replace(vpcCatalogDCIT, 'Vpc_Desarrollo_Op') \
            .na.replace(vpcCatalogDCIT, 'Vpc_Fidelizacion_Op')

        usersOutputDF = usersCleanDF \
            .na.fill("-1", ["Vpc_Desarrollo_Op"]) \
            .na.fill("-1", ["Vpc_Fidelizacion_Op"])

        # Commons.writeInformationTera(usersOutputDF, 'UserLayout')

        return 'OK'

######################################################
#   LOGIN MAIN
######################################################
class BusinessLogic:

    @staticmethod
    def logic_main(loadOnlyDelta = 'yes'):
        logger = CommonsUtils.getLoggerModule('logic_main')
        resultFinally = 'OK'

        print('----------------------------------------------------')
        print('CARGA INICIADA ....')
        print('----------------------------------------------------')

        print('Carga solo delta : ', loadOnlyDelta)

        try:
            dateNow = datetime.utcnow().date()
            dateLimit = dateNow - timedelta(1)
            dateBegin = dateLimit
            dateEnd = dateLimit

            if (loadOnlyDelta != 'yes'):
                dateBegin = '2020-01-01'
                dateEnd = dateNow

            print('Fecha Inicio: ', dateBegin)
            print('Fecha Fin: ', dateEnd)

            print('\nCATALOGOS .......')
            print('CatalogLayout: ' + CatalogLayout.execute(dateBegin, dateEnd))
            #print('UserLayout: ' + UserLayout.execute(dateBegin, dateEnd))
        except Exception as e:
            #NotControledException
            print(str(e))
            # logger.error(str(e))
            resultFinally = 'ERROR'

        print('----------------------------------------------------')
        print('CARGA TERMINADA .... ', resultFinally)
        print('----------------------------------------------------')


if __name__ == '__main__':
    loadOnlyDelta = 'no'
    BusinessLogic.logic_main(loadOnlyDelta)

