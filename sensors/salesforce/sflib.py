from simple_salesforce import Salesforce, SFType
from simple_salesforce.format import format_soql
try:
    import destinations
except: 
    pass # for unit tests. They will import destinations another way.

import unittest, yaml, json, os, os.path, tempfile, sys

column_type_map = {
    'bq': yaml.safe_load('''
boolean: boolean #3103 indicates how many entries existed in the default db with this column type
reference: string #3062
string: string #2651
datetime: string #2497
picklist: string #1511
id: string #856
int: int64 #605
textarea: string #432
double: float64 #297
currency: float64 #294
anyType: string #176
url: string #153
date: date #144
phone: string #72
complexvalue: string #65
address: string #51
email: string #42
time: string #28
percent: float64 #19
json: string #16
base64: string #9
combobox: string #8
multipicklist: string #5
encryptedstring: string #4
long: int64 # 2
'''),
                   }

def instantiate_from_creds(creds):
    sfcreds = json.loads(creds)
    return Salesforce(username=sfcreds['username'],
                      password=sfcreds['password'],
                      security_token=sfcreds['security_token'])
    
def get_tables(sf):
    load_describe(sf)
    return [x['name'] for x in sf._trel_describe["sobjects"]]

def load_describe(sf, force=False):
    if force or type(sf._trel_describe) == SFType:
        sf._trel_describe = sf.describe()

def get_table(sf, table):
    table_data = getattr(sf, table).describe()
    if type(table_data) == SFType:
        raise Exception(f"Table {table} not found.")
    return table_data

def extract_table_columns(table_data):
    return [ (col['name'],col['type']) for col in table_data['fields'] ]

def get_data_iterable(sf, table_data, cols = None):
    cols_str = 'Id' if cols is None else ', '.join(cols)
    # improve iteration block size
    return sf.query_all_iter(f"select {cols_str} from {table_data['name']}")

class TableNotQueryableException(Exception):
    pass

def load_table(sf, table_name, uri, sensor, cols=None):
    ''' Copies the given table from the given salesforce instance to the provided URI using destination classes'''
    
    table_data = get_table(sf, table_name)
    if not table_data['queryable']:
        raise TableNotQueryableException(f"Specified table {table_name} is not queryable.")
    table_cols = dict(extract_table_columns(table_data))
    print(table_cols)
    if cols is not None:
        if len(set(cols).difference(table_cols)) > 0:
            raise Exception(f"The columns {set(cols).difference(table_cols)} that you requested do not existing in table {table_name}")
    extraction_columns = list(table_cols) if cols is None else cols

    data_it = get_data_iterable(sf, table_data, extraction_columns)
    destination = destinations.DestinationProtocol.get_object_from_uri(uri, sensor)
    if destination.get_schema_format_name() in column_type_map:
        destination_type_map = column_type_map[destination.get_schema_format_name()]
        destination_schema = []
        for column in extraction_columns:
            type_ = table_cols[column]
            column_destination_type = destination_type_map[type_]
            destination_schema.append({'name': column,
                                       'type': column_destination_type,
                                       'nullable': True})
        destination.set_column_format(destination_schema)
    else:
        if destination.requires_column_schema:
            raise Exception("Destination requires column schema, but none provided.")

    destination.prepare()

    #print(f"Executed SQL:\n{cursor._last_executed}", file=sys.stderr)
    fetch_rows = 100000 # batch size
    done = False
    while not done:
        with tempfile.NamedTemporaryFile('w+', delete=False) as f:
            row_i = 0
            done = True
            for row in data_it:
                done = False
                del row['attributes']
                #print(row)
                destination.write_dict_row_to_file(row, f)
                row_i += 1
                if row_i >= fetch_rows:
                    break
            if done:
                break
        print(f"Uploading batch {destination.get_next_batch_num()} data from {f.name} to {uri}",file=sys.stderr)
        destination.append_data(f.name)
        sys.stderr.flush()
    destination.finish()
    

class Test(unittest.TestCase):

    def match_failed_tables(self):
        '''When querying against Salesforce dev instance created as of 12/30, got 881 Tables.When trying to load them, 130 failed to load. Of which 97 were not queryable, explaining the failure. In the remaining 33, 1 was a fixable type casting. 2 were due to data permissions for data.com and the rest were due to some requirement for querying the table suggesting that this is not suitable for loading to a data warehouse.

        Based on this analysis, These 32 tables and any tables that are marked as not "queryable" will be excluded from the sensor.
        '''
        failed = ['AIPredictionEvent', 'AccountChangeEvent', 'AccountContactRoleChangeEvent', 'ActivityHistory', 'AggregateResult', 'ApiAnomalyEvent', 'ApiEventStream', 'AppTabMember', 'AssetChangeEvent', 'AssetTokenEvent', 'AssignedResourceChangeEvent', 'AsyncOperationEvent', 'AsyncOperationStatus', 'AttachedContentDocument', 'AuthorizationFormConsentChangeEvent', 'BatchApexErrorEvent', 'BulkApiResultEvent', 'BusinessHours', 'CampaignChangeEvent', 'CampaignMemberChangeEvent', 'CampaignMemberStatusChangeEvent', 'CaseChangeEvent', 'ColorDefinition', 'CombinedAttachment', 'CommSubscriptionConsentChangeEvent', 'ConcurLongRunApexErrEvent', 'ContactChangeEvent', 'ContactPointAddressChangeEvent', 'ContactPointConsentChangeEvent', 'ContactPointEmailChangeEvent', 'ContactPointPhoneChangeEvent', 'ContactPointTypeConsentChangeEvent', 'ContentBody', 'ContentDocumentLink', 'ContentFolderItem', 'ContentFolderMember', 'ContractChangeEvent', 'ContractLineItemChangeEvent', 'CredentialStuffingEvent', 'DataStatistics', 'DataType', 'DatacloudAddress', 'DatacloudCompany', 'DatacloudContact', 'DatacloudDandBCompany', 'EmailMessageChangeEvent', 'EmailStatus', 'EmailTemplateChangeEvent', 'EntitlementChangeEvent', 'EntityParticle', 'EventChangeEvent', 'EventRelationChangeEvent', 'FeedLike', 'FeedSignal', 'FeedTrackedChange', 'FieldDefinition', 'FinanceBalanceSnapshotChangeEvent', 'FinanceTransactionChangeEvent', 'FlexQueueItem', 'FlowExecutionErrorEvent', 'FlowVariableView', 'FlowVersionView', 'FolderedContentDocument', 'IconDefinition', 'IdeaComment', 'IndividualChangeEvent', 'LeadChangeEvent', 'LightningUriEventStream', 'ListEmailChangeEvent', 'ListViewChartInstance', 'ListViewEventStream', 'LocationChangeEvent', 'LoginAsEventStream', 'LoginEventStream', 'LogoutEventStream', 'LookedUpFromActivity', 'MacroChangeEvent', 'MacroInstructionChangeEvent', 'Name', 'NoteAndAttachment', 'OpenActivity', 'OpportunityChangeEvent', 'OpportunityContactRoleChangeEvent', 'OrderChangeEvent', 'OrderItemChangeEvent', 'OrgLifecycleNotification', 'OutgoingEmail', 'OutgoingEmailRelation', 'OwnedContentDocument', 'OwnerChangeOptionInfo', 'PartyConsentChangeEvent', 'PermissionSetEvent', 'PicklistValueInfo', 'PlatformAction', 'PlatformStatusAlertEvent', 'Pricebook2ChangeEvent', 'ProcessExceptionEvent', 'ProcessInstanceHistory', 'Product2ChangeEvent', 'QuickTextChangeEvent', 'QuoteTemplateRichTextData', 'RecommendationChangeEvent', 'RelationshipDomain', 'RelationshipInfo', 'RemoteKeyCalloutEvent', 'ReportAnomalyEvent', 'ReportEventStream', 'ResourceAbsenceChangeEvent', 'ReturnOrderChangeEvent', 'ReturnOrderLineItemChangeEvent', 'SearchLayout', 'ServiceAppointmentChangeEvent', 'ServiceContractChangeEvent', 'ServiceResourceChangeEvent', 'ServiceTerritoryChangeEvent', 'ServiceTerritoryMemberChangeEvent', 'SessionHijackingEvent', 'SiteDetail', 'TaskChangeEvent', 'UriEventStream', 'UserChangeEvent', 'UserEntityAccess', 'UserFieldAccess', 'UserRecordAccess', 'VoiceCallChangeEvent', 'VoiceCallRecordingChangeEvent', 'Vote', 'WorkOrderChangeEvent', 'WorkOrderLineItemChangeEvent', 'WorkTypeChangeEvent']
        import sflib, json, os, yaml

        with open(os.path.expanduser('~/.trel_test_credentials.yml')) as f:
            creds = yaml.safe_load(f)
        sf = instantiate_from_creds(creds['salesforce'])
        load_describe(sf)
        so  =sf._trel_describe["sobjects"]
        print("Total tables",len(so))
        print("Total tables failed loading",len(failed))
        not_retrievable = set([v['name'] for v in so if not v['retrieveable']])
        not_queryable = set([v['name'] for v in so if not v['queryable']])
        print("not_queryable", len(not_queryable))
        failed = set(failed)
        print("failed - not_retrievable", failed.difference(not_retrievable))
        print()
        print("not_retrievable - failed", not_retrievable.difference(failed))
        print()
        print("failed - not_queryable", failed.difference(not_queryable))
        print()
        print("not_queryable - failed", not_queryable.difference(failed))

    
    def test(self):
        import yaml, os
        with open(os.path.expanduser('~/.trel_test_credentials.yml')) as f:
            creds = yaml.safe_load(f)
        sf = instantiate_from_creds(creds['salesforce'])
            
        load_describe(sf)
        print(get_tables(sf))
        #extract_data(sf, get_table(sf, "Account"), ['Name', 'AnnualRevenue'])
        sf.session.close()
    
    def test_types(self):
        import yaml, os
        with open(os.path.expanduser('~/.trel_test_credentials.yml')) as f:
            creds = yaml.safe_load(f)
        sf = instantiate_from_creds(creds['salesforce'])

        type_counts = {}
        
        for i, table in enumerate(get_tables(sf)):
            print(i, "extracting metadata about", table)
            for name, type_ in extract_table_columns(get_table(sf, table)):
                type_counts[type_] = type_counts.get(type_,0) + 1
        type_counts_sorted = sorted(list(type_counts.items()), key=lambda x: -x[1])
        print('\n'.join(f"{type_}: {c}" for type_, c in type_counts_sorted))
        sf.session.close()

    def test_bq_load(self):
        import yaml, os, sys, os.path, tempfile
        sys.path.append('..')
        global destinations
        import destinations
        import treldev

        treldev.SQLExecutor.credentials_file = os.path.expanduser('~/.trel_test_credentials.yml')
        with open(os.path.expanduser('~/.trel_test_credentials.yml')) as f:
            creds = yaml.safe_load(f)
        sf = instantiate_from_creds(creds['salesforce'])
        load_describe(sf)
        failed_tables = []
        non_queryable_tables = []
        for i,table_name in enumerate(get_tables(sf)):
            if table_name in set(recommended_excluded_tables):
                continue
            try:
                print("processing [",i,"] table", table_name)
                load_table(sf, table_name, 'bq://trel-main/tmp/sflib_load_'+table_name, self)
            except TableNotQueryableException as ex:
                print(ex)
                non_queryable_tables.append(table_name)
            except Exception as ex:
                print(ex)
                failed_tables.append(table_name)
        print("failed tables", failed_tables)
        print("non queryable tables", non_queryable_tables)
        sf.session.close()
        self.assertEqual(set(failed_tables).difference(recommended_excluded_tables),set())


''' Exclude these tables from the load '''        
recommended_excluded_tables = ['FlowVersionView', 'AppTabMember', 'FlowVariableView', 'ContentDocumentLink', 'Vote', 'IdeaComment', 'UserRecordAccess', 'RelationshipInfo', 'UserEntityAccess', 'ContentFolderItem', 'DataStatistics', 'FlexQueueItem', 'ListViewChartInstance', 'RelationshipDomain', 'FieldDefinition', 'EntityParticle', 'DataType', 'IconDefinition', 'DatacloudContact', 'DatacloudCompany', 'ContentFolderMember', 'OutgoingEmail', 'DatacloudDandBCompany', 'PlatformAction', 'SearchLayout', 'OwnerChangeOptionInfo', 'PicklistValueInfo', 'ColorDefinition', 'SiteDetail', 'DatacloudAddress', 'UserFieldAccess', 'OutgoingEmailRelation']
