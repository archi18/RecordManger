#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "tables.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"

void readSchemaFromFile(RM_TableData *, BM_PageHandle *);
char * readSchemaName(char *);
char * readAttributeMeataData(char *);
int readTotalKeyAttr(char *);
char * readAttributeKeyData(char *);
char * getSingleAtrData(char *, int );
char ** getAtrributesNames(char *, int );
char * extractName(char *);
int extractDataType(char *);
int * getAtributesDtType(char *, int );
int extractTypeLength(char *data);
int * getAtributeSize(char *scmData, int numAtr);
int * extractKeyDt(char *data,int keyNum);
int * extractFirstFreePageSlot(char *);
char * readFreePageSlotData(char *);

typedef struct TableMgmt_info{
    int sizeOfRec;
    int totalRecordInTable;
    int blkFctr;
    RID firstFreeLoc;
    RM_TableData *rm_tbl_data;
}TableMgmt_info;

TableMgmt_info tblmgmt_info;
SM_FileHandle fh;
SM_PageHandle ph;

RC initRecordManager (void *mgmtData){
    initStorageManager();
    printf("*******************************Record manager Initialized********************************");
    return RC_OK;
}

RC shutdownRecordManager (){

    return RC_OK;
}

RC createTable (char *name, Schema *schema){

    if(createPageFile(name) != RC_OK){
        return 1;
    }
    ph = (SM_PageHandle) malloc(PAGE_SIZE);

    char tableMetadata[PAGE_SIZE];
    memset(tableMetadata,'\0',PAGE_SIZE);

    sprintf(tableMetadata,"%s|",name);

    int recordSize = getRecordSize(schema);
    sprintf(tableMetadata+ strlen(tableMetadata),"%d[",schema->numAttr);

    for(int i=0; i<schema->numAttr; i++){
        sprintf(tableMetadata+ strlen(tableMetadata),"(%s:%d~%d)",schema->attrNames[i],schema->dataTypes[i],schema->typeLength[i]);
    }
    sprintf(tableMetadata+ strlen(tableMetadata),"]%d{",schema->keySize);

    for(int i=0; i<schema->keySize; i++){
        sprintf(tableMetadata+ strlen(tableMetadata),"%d",schema->keyAttrs[i]);
        if(i<(schema->keySize-1))
            strcat(tableMetadata,":");
    }
    strcat(tableMetadata,"}");

    tblmgmt_info.firstFreeLoc.page =1;
    tblmgmt_info.firstFreeLoc.slot =0;

    sprintf(tableMetadata+ strlen(tableMetadata),"$%d:%d$",tblmgmt_info.firstFreeLoc.page,tblmgmt_info.firstFreeLoc.slot);
    memmove(ph,tableMetadata,PAGE_SIZE);

    if (openPageFile(name, &fh) != RC_OK) {
        return 1;
    }

    if (writeBlock(0, &fh, ph)!= RC_OK) {
        return 1;
    }

    printf("\n tablemeatadata :: %s",ph);
    free(ph);
    return RC_OK;
}

RC openTable (RM_TableData *rel, char *name){
    ph = (SM_PageHandle) malloc(PAGE_SIZE);

    BM_PageHandle *h = MAKE_PAGE_HANDLE();

    BM_BufferPool *bm = MAKE_POOL();

    initBufferPool(bm, name, 3, RS_FIFO, NULL);

    if(pinPage(bm, h, 0) != RC_OK){
        RC_message = "Pin page failed ";
        return RC_PIN_PAGE_FAILED;
    }

    printf("\n pin page data %s",h->data);
    printf("\n pin page number %d",h->pageNum);

    readSchemaFromFile(rel,h);

    printf("\n Free pageSlot %d : %d",tblmgmt_info.firstFreeLoc.page,tblmgmt_info.firstFreeLoc.slot);

    if(unpinPage(bm,h) != RC_OK){
        RC_message = "Unpin page failed ";
        return RC_UNPIN_PAGE_FAILED;
    }

    return RC_OK;
}

RC closeTable (RM_TableData *rel){

    return RC_OK;
}

RC deleteTable (char *name){

    return RC_OK;
}

int getNumTuples (RM_TableData *rel){

    return RC_OK;
}


// handling records in a table
RC insertRecord (RM_TableData *rel, Record *record){

    return RC_OK;
}

RC deleteRecord (RM_TableData *rel, RID id){

    return RC_OK;
}

RC updateRecord (RM_TableData *rel, Record *record){

    return RC_OK;
}

RC getRecord (RM_TableData *rel, RID id, Record *record){

    return RC_OK;
}


// scans
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond){

    return RC_OK;
}

RC next (RM_ScanHandle *scan, Record *record){

    return RC_OK;
}

RC closeScan (RM_ScanHandle *scan){

    return RC_OK;
}


// dealing with schemas
int getRecordSize (Schema *schema){
    if(schema ==((Schema *)0)){
        RC_message = "schema is not initialized.Create schema first ";
        return RC_SCHEMA_NOT_INIT;
    }
    int recordSize = 0;

    for(int i=0; i<schema->numAttr; i++){
        switch(schema->dataTypes[i]){
            case DT_INT:
                recordSize = recordSize + sizeof(int);
                break;
            case DT_STRING:
                recordSize = recordSize + (sizeof(char) * schema->typeLength[i]);
                break;
            case DT_FLOAT:
                recordSize = recordSize + sizeof(float);
                break;
            case DT_BOOL:
                recordSize = recordSize + sizeof(short);
                break;
        }
    }
    return recordSize;
}

Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys){

    Schema *schema = (Schema * ) malloc(sizeof(Schema));

    if(schema ==((Schema *)0)) {
        RC_message = "dynamic memory allocation failed | schema";
        return RC_MELLOC_MEM_ALLOC_FAILED;
    }

    schema->numAttr = numAttr;
    schema->attrNames = attrNames;
    schema->dataTypes = dataTypes;
    schema->typeLength = typeLength;
    schema->keySize = keySize;
    schema->keyAttrs = keys;
    int recordSize = getRecordSize(schema);
    tblmgmt_info.sizeOfRec = recordSize;
    tblmgmt_info.totalRecordInTable = 0;

    return schema;
}

RC freeSchema (Schema *schema){

    return RC_OK;
}


// dealing with records and attribute values
RC createRecord (Record **record, Schema *schema){

    return RC_OK;
}

RC freeRecord (Record *record){

    return RC_OK;
}

RC getAttr (Record *record, Schema *schema, int attrNum, Value **value){

    return RC_OK;
}

RC setAttr (Record *record, Schema *schema, int attrNum, Value *value){

    return RC_OK;
}

void readSchemaFromFile(RM_TableData *rel, BM_PageHandle *h){
    char metadata[PAGE_SIZE];
    strcpy(metadata,h->data);
    printf("\nschema data : %s",metadata);
    char *schema_name=readSchemaName(&metadata);
    printf("\n read table name : %s",schema_name);

    int totalAtribute = readTotalAttributes(&metadata);
    printf("\n tatal no of Attributes : %d",totalAtribute);

    char *atrMetadata =readAttributeMeataData(&metadata);
    printf("\n Attributes Data : %s",atrMetadata);

    int totalKeyAtr = readTotalKeyAttr(&metadata);
    printf("\n Total key attr : %d",totalKeyAtr);

    char *atrKeydt = readAttributeKeyData(&metadata);
    printf("\n  key attr data: %s",atrKeydt);

    char *freeVacSlot = readFreePageSlotData(&metadata);
    printf("\n Vacancy Data %s",freeVacSlot);


 /*   getAtrributesNames(atrMetadata,totalAtribute);
    getAtributesDtType(atrMetadata,totalAtribute);
    getAtributeSize(atrMetadata,totalAtribute);
    extractKeyDt(atrKeydt,totalKeyAtr);*/

    char **names=getAtrributesNames(atrMetadata,totalAtribute);
    DataType *dt =   getAtributesDtType(atrMetadata,totalAtribute);
    int *sizes = getAtributeSize(atrMetadata,totalAtribute);
    int *keys =   extractKeyDt(atrKeydt,totalKeyAtr);
    int *pageSolt = extractFirstFreePageSlot(freeVacSlot);

    int i;
    char **cpNames = (char **) malloc(sizeof(char*) * totalAtribute);
    DataType *cpDt = (DataType *) malloc(sizeof(DataType) * totalAtribute);
    int *cpSizes = (int *) malloc(sizeof(int) * totalAtribute);
    int *cpKeys = (int *) malloc(sizeof(int)*totalKeyAtr);
    char *cpSchemaName = (char *) malloc(sizeof(char)*20);

    memset(cpSchemaName,'\0',sizeof(char)*20);

    for(int i = 0; i < totalAtribute; i++)
    {
        cpNames[i] = (char *) malloc(sizeof(char) * 10);
         strcpy(cpNames[i], names[i]);
        printf(" \n atr : %s %d %d ",names[i],dt[i],sizes[i]);
    }
    for(int i=0; i<totalKeyAtr; i++){
        printf(" \n keys %d",keys[i]);
    }
    memcpy(cpDt, dt, sizeof(DataType) * totalAtribute);
    memcpy(cpSizes, sizes, sizeof(int) * totalAtribute);
    memcpy(cpKeys, keys, sizeof(int) * totalKeyAtr);
    memcpy(cpSchemaName,schema_name,strlen(schema_name));

    free(names);
    free(dt);
    free(sizes);
    free(keys);
    free(schema_name);

    Schema *schema = createSchema(totalAtribute, cpNames, cpDt, cpSizes, totalKeyAtr, cpKeys);
    rel->schema=schema;
    rel->name =cpSchemaName;

    tblmgmt_info.rm_tbl_data = rel;
    tblmgmt_info.sizeOfRec =  getRecordSize(rel->schema);
    tblmgmt_info.blkFctr = PAGE_SIZE / tblmgmt_info.sizeOfRec;
    tblmgmt_info.firstFreeLoc.page =pageSolt[0];
    tblmgmt_info.firstFreeLoc.slot =pageSolt[1];

    printf(" \n Record Size %d ",tblmgmt_info.sizeOfRec);
    printf(" \n Blocking factor %d ",tblmgmt_info.blkFctr);

   printf(" \n after schema creation %s",rel->name);
}


char * readSchemaName(char *scmData){
    char *tbleName = (char *) malloc(sizeof(char)*20);
    memset(tbleName,'\0',sizeof(char)*20);
    int i=0;
    for(i=0; scmData[i] != '|'; i++){
        tbleName[i]=scmData[i];
    }
    tbleName[i]='\0';
 //   printf("\n table name : %s",tbleName);
    return tbleName;
}

int readTotalAttributes(char *scmData){
    char *strNumAtr = (char *) malloc(sizeof(int)*2);
    memset(strNumAtr,'\0',sizeof(int)*2);
    int i=0;
    int j=0;
    for(i=0; scmData[i] != '|'; i++){
    }
    i++;
    while(scmData[i] != '['){
       // printf("\n num :: %c",scmData[i]);
        strNumAtr[j++]=scmData[i++];
    }
    strNumAtr[j]='\0';
   // printf(" \n num attributes :%s",strNumAtr);
    return atoi(strNumAtr);
}


int readTotalKeyAttr(char *scmData){
    char *strNumAtr = (char *) malloc(sizeof(int)*2);
    memset(strNumAtr,'\0',sizeof(int)*2);
    int i=0;
    int j=0;
    for(i=0; scmData[i] != ']'; i++){
    }
    i++;
    while(scmData[i] != '{'){
        // printf("\n num :: %c",scmData[i]);
        strNumAtr[j++]=scmData[i++];
    }
    strNumAtr[j]='\0';
  //  printf(" \n num attributes :%s",strNumAtr);
    return atoi(strNumAtr);
}

char * readAttributeMeataData(char *scmData){
    char *atrData = (char *) malloc(sizeof(char)*100);
    memset(atrData,'\0',sizeof(char)*100);
    int i=0;
    while(scmData[i] != '['){
        i++;
    }
    i++;
    int j=0;
    for(j=0;scmData[i] != ']'; j++){
        atrData[j] = scmData[i++];
    }
    atrData[j]='\0';
   // printf(" Attribute data : %s ",atrData);

    return atrData;

}


char * readAttributeKeyData(char *scmData){
    char *atrData = (char *) malloc(sizeof(char)*50);
    memset(atrData,'\0',sizeof(char)*50);
    int i=0;
    while(scmData[i] != '{'){
        i++;
    }
    i++;
    int j=0;
    for(j=0;scmData[i] != '}'; j++){
        atrData[j] = scmData[i++];
    }
    atrData[j]='\0';
   // printf(" Attribute data : %s ",atrData);

    return atrData;
}

char * readFreePageSlotData(char *scmData){
    char *atrData = (char *) malloc(sizeof(char)*50);
    memset(atrData,'\0',sizeof(char)*50);
    int i=0;
    while(scmData[i] != '$'){
        i++;
    }
    i++;
    int j=0;
    for(j=0;scmData[i] != '$'; j++){
        atrData[j] = scmData[i++];
    }
    atrData[j]='\0';
    // printf(" Attribute data : %s ",atrData);

    return atrData;
}

Schema * getAtrDetails(char *scmData, int numAtr, char *keydata, int numKeys){
    Schema *result;
    char *names[numAtr];
    DataType dt[numAtr];
    int sizes[numAtr];
    int keys[numKeys];



    int i;
    char **cpNames = (char **) malloc(sizeof(char*) * 3);
    DataType *cpDt = (DataType *) malloc(sizeof(DataType) * 3);
    int *cpSizes = (int *) malloc(sizeof(int) * 3);
    int *cpKeys = (int *) malloc(sizeof(int));

    for(i = 0; i < numAtr; i++)
    {
        cpNames[i] = (char *) malloc(4);
        strcpy(cpNames[i], names[i]);
    }
    memcpy(cpDt, dt, sizeof(DataType) * numAtr);
    memcpy(cpSizes, sizes, sizeof(int) * numAtr);
    memcpy(cpKeys, keys, sizeof(int));

    result = createSchema(3, cpNames, cpDt, cpSizes, numKeys, cpKeys);

    return  result;
}


char ** getAtrributesNames(char *scmData, int numAtr){
    //char *attributesName[numAtr];
    char ** attributesName = (char **) malloc(sizeof(char)*numAtr);
    for(int i=0; i<numAtr; i++){
        char *atrDt =getSingleAtrData(scmData,i);
      //  printf("\n signle data %s",atrDt);
        char *name = extractName(atrDt);
        //printf(" \n atr name is %s",name);
        attributesName[i] = malloc(sizeof(char) * strlen(name));
        strcpy(attributesName[i],name);
        printf(" \n atr name is %s",attributesName[i]);
        free(name);
        free(atrDt);
    }
    return attributesName;
}

int * getAtributesDtType(char *scmData, int numAtr){
    int *dt_type=(int *) malloc(sizeof(int) *numAtr);

    for(int i=0; i<numAtr; i++){
        char *atrDt =getSingleAtrData(scmData,i);
        dt_type[i]  = extractDataType(atrDt);
        printf("\n data type %d",dt_type[i]);
        free(atrDt);
    }
    return dt_type;
}

int * getAtributeSize(char *scmData, int numAtr){
    int *dt_size= (int *) malloc(sizeof(int) *numAtr);

    for(int i=0; i<numAtr; i++){
        char *atrDt =getSingleAtrData(scmData,i);
        dt_size[i]  = extractTypeLength(atrDt);
        printf("\n data size %d",dt_size[i]);
        free(atrDt);
    }

    return dt_size;
}

char * getSingleAtrData(char *scmData, int atrNum){
    char *atrData = (char *) malloc(sizeof(char)*30);
    int count=0;
    int i=0;
    while(count<=atrNum){
        if(scmData[i++] == '(')
            count++;
    }
   // i++;
    int j=0;
    for(j=0;scmData[i] != ')'; j++){
        atrData[j] = scmData[i++];
    }
    atrData[j]='\0';
    return atrData;
}


char * extractName(char *data){
    char *name = (char *) malloc(sizeof(char)*10);
    memset(name,'\0',sizeof(char)*10);
    int i;
    for(i=0; data[i]!=':'; i++){
        name[i] = data[i];
    }
    name[i]='\0';
    return  name;
}

int extractDataType(char *data){
    char *dtTp = (char *) malloc(sizeof(int)*2);
    memset(dtTp,'\0',sizeof(char)*10);
    int i;
    int j;
    for(i =0 ; data[i]!=':'; i++){
    }
    i++;
    for(j=0; data[i]!='~'; j++){
        dtTp[j]=data[i++];
    }
    //printf("\n data Type [%s]",dtTp);
    dtTp[j]='\0';
    int dt =atoi(dtTp);
    free(dtTp);
    return  dt;
}

int extractTypeLength(char *data){
    char *dtLen = (char *) malloc(sizeof(int)*2);
    memset(dtLen,'\0',sizeof(char)*10);
    int i;
    int j;
    for(i =0 ; data[i]!='~'; i++){
    }
    i++;
    for(j=0; data[i]!='\0'; j++){
        dtLen[j]=data[i++];
    }
    //printf("\n data Type [%s]",dtTp);
    dtLen[j]='\0';
    int dt =atoi(dtLen);
    free(dtLen);
    return  dt;
}

int * extractKeyDt(char *data,int keyNum){
    char *val = (char *) malloc(sizeof(int)*2);
    int * values=(int *) malloc(sizeof(int) *keyNum);
    memset(val,'\0',sizeof(int)*2);
    int i=0;
    int j=0;
    for(int k=0; data[k]!='\0'; k++){
        if(data[k]==':' ){
            values[j]=atoi(val);
            memset(val,'\0',sizeof(int)*2);
            printf("\n key value %d",values[j]);
            i=0;
            j++;

        }else{
            val[i++] = data[k];
        }

    }
    values[keyNum-1] =atoi(val);
    printf("\n key value last %d",values[keyNum-1]);

    return  values;
}


int * extractFirstFreePageSlot(char *data){
    char *val = (char *) malloc(sizeof(int)*2);
    int * values=(int *) malloc(sizeof(int) *2);
    memset(val,'\0',sizeof(int)*2);
    int i=0;
    int j=0;
    for(int k=0; data[k]!='\0'; k++){
        if(data[k]==':' ){
            values[j]=atoi(val);
            memset(val,'\0',sizeof(int)*2);
            printf("\n page value %d",values[j]);
            i=0;
            j++;

        }else{
            val[i++] = data[k];
        }

    }
    values[1] =atoi(val);
    printf("\n Slot %d",values[1]);
    return  values;
}
