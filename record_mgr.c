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
int getAtrOffsetInRec(Schema *, int );

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

    char *pageData;   //user for convinient to hangle page data , do not use malloc and free. its is pointer.
    int recordSize = tblmgmt_info.sizeOfRec;

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
                recordSize = recordSize + sizeof(bool);
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

    Record *newRec = (Record *) malloc (sizeof(Record));
    if(newRec == ((Record *)0)){
        RC_message = "dynamic memory allocation failed | Record";
        return RC_MELLOC_MEM_ALLOC_FAILED;
    }
    //printf(" \n record Size %d",tblmgmt_info.sizeOfRec);
    newRec->data = (char *)malloc(sizeof(char) * tblmgmt_info.sizeOfRec);
    memset(newRec->data,'\0',sizeof(char) * tblmgmt_info.sizeOfRec);
    printf("\n Newly created record ");
    printRecord(newRec->data, tblmgmt_info.sizeOfRec);

    newRec->id.page =-1;           //set to -1 bcz it has not inserted into table/page/slot
    newRec->id.page =-1;           //set to -1 bcz it has not inserted into table/page/slot
    printf(" \n data innewly created data %s",newRec->data);


    *record = newRec;

    return RC_OK;
}

RC freeRecord (Record *record){
    free(record);
    return RC_OK;
}
/*
 *
 * In case of error check  free(subString); try commenting it
 *
 *
 * */
RC getAttr (Record *record, Schema *schema, int attrNum, Value **value){
    int offset = getAtrOffsetInRec(schema,attrNum);

    printf("\n atrribute offset of %d = %d ",attrNum,offset);

    Value *l_rec;
    int int_atr;
    float float_atr;
    int atr_size =0;
    char *subString ;
    

    switch (schema->dataTypes[attrNum]) {
        case DT_INT :
            atr_size = sizeof(int);
            subString= malloc(atr_size+1);     // one extra byte to store '\0' char
            memcpy(subString, record->data+offset, atr_size);
            subString[atr_size]='\0';          // set last byet to '\0'
            printf("\n  substring %s",subString);
            int_atr =  atoi(subString);
            printf("\n  int_atr : %d",int_atr);

            MAKE_VALUE(*value, DT_INT, int_atr);
            free(subString);
            break;
        case DT_STRING :
            atr_size =sizeof(char)*schema->typeLength[attrNum];
            subString= malloc(atr_size+1);    // one extra byte to store '\0' char
            memcpy(subString, record->data+offset, atr_size);
            subString[atr_size]='\0';       // set last byet to '\0'
            printf("\n  substring %s",subString);

            MAKE_STRING_VALUE(*value, subString);
            free(subString);
            break;
        case DT_FLOAT :
            atr_size = sizeof(float);
            subString= malloc(atr_size+1);     // one extra byte to store '\0' char
            memcpy(subString, record->data+offset, atr_size);
            subString[atr_size]='\0';          // set last byet to '\0'
            printf("\n  substring %s",subString);
            float_atr =  atof(subString);
            printf("\n  flt_atr : %f",float_atr);

            MAKE_VALUE(*value, DT_FLOAT, float_atr);
            free(subString);
            break;
        case DT_BOOL :
            atr_size = sizeof(bool);
            subString= malloc(atr_size+1);     // one extra byte to store '\0' char
            memcpy(subString, record->data+offset, atr_size);
            subString[atr_size]='\0';          // set last byet to '\0'
            printf("\n  substring %s",subString);
            int_atr =  atoi(subString);
            printf("\n  int_atr : %d",int_atr);

            MAKE_VALUE(*value, DT_BOOL, int_atr);
            free(subString);
            break;
    }
    // remember to return pointer of new value
   // *value = l_rec;
    return RC_OK;

    return RC_OK;
}
/*
 *
 * sets value for perticular atrribute
 * note : check string value in case of error : last value to be set to '\0' or not
 * */
RC setAttr (Record *record, Schema *schema, int attrNum, Value *value){

    int offset = getAtrOffsetInRec(schema,attrNum);

    printf("\n atrribute offset of %d = %d ",attrNum,offset);
    printf("\n old record value %s",record->data);
    //record->data = record->data +offset;
    char intStr[sizeof(int)+1];
    char intStrTemp[sizeof(int)+1];
    int remainder = 0,quotient = 0;
    int k = 0,j;
    bool q,r;

    memset(intStr,'0',sizeof(char)*4);
    char *hexValue ="0001";
    int number = (int)strtol(hexValue, NULL, 16);

       switch(schema->dataTypes[attrNum])
       {
           case DT_INT: {

               quotient = value->v.intV;
               j = 3;
               while (quotient > 0 && j >= 0) {
                   remainder = quotient % 10;
                   quotient = quotient / 10;
                   intStr[j] = intStr[j] + remainder;

                   j--;
               }
               intStr[4] = '\0';
             //  printf("\n int to hex %x ", intStr);
               sprintf(record->data + offset, "%s", intStr);
           }
               break;
           case DT_STRING: {
               int strLength =schema->typeLength[attrNum];
               sprintf(record->data + offset, "%s", value->v.stringV);
               char buf[strLength];
               memset(&buf,'\0',strLength);
               for(int i=strLength-1, j =strlen(value->v.stringV)-1; i>=(strLength-strlen(value->v.stringV)); i--,j--){
                    buf[i]=value->v.stringV[j];
               }
               printRecord(&buf,strLength);
               break;
           }
           case DT_FLOAT:
               sprintf(record->data + offset,"%f" ,value->v.floatV);
               break;
           case DT_BOOL:
               q = value->v.boolV;
               j = 1;
               while(q > 0 && j >= 0 ){
                   r = q % 10;
                   q = q / 10;
                   intStr[j] = intStr[j] + r;
                   j--;
               }
               intStr[2] = '\0';
               sprintf(record->data + offset,"%s" ,intStr);
               break;
       }

    printf("\n New record value ");
    printRecord(record->data,tblmgmt_info.sizeOfRec);
    printf("\n Record Size %d",strlen(record->data));
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
    tblmgmt_info.sizeOfRec =  getRecordSize(rel->schema) + 1;   //
    tblmgmt_info.blkFctr = (PAGE_SIZE / tblmgmt_info.sizeOfRec);
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

/*
 *   returns offset/string posing of perticular attribute
 *   atrnum is offset we are seeking for
 */
int getAtrOffsetInRec(Schema *schema, int atrnum){
    int offset=0;

    for(int pos=0; pos<atrnum; pos++){
        switch(schema->dataTypes[pos]){
            case DT_INT:
                    offset = offset + sizeof(int);
                break;
            case DT_STRING:
                   offset = offset + (sizeof(char) *  schema->typeLength[pos]);
                break;
            case DT_FLOAT:
                   offset = offset + sizeof(float);
                break;
            case DT_BOOL:
                  offset = offset  + sizeof(bool);
                break;
        }
    }

    return offset;
}


void printRecord(char *record, int recLen){
    for(int i=0; i<recLen; i++){
        printf("%c",record[i]);
    }
}

/*
 * Use to print print Record deatils to make sure integrity of information and persistancy data
 *
 * */
void printTableInfoDetails(TableMgmt_info *tab_info){
    printf(" \n Printing record details ");
    printf(" \n table name [%s]",tab_info->rm_tbl_data->name);
    printf(" \n Size of record [%d]",tab_info->sizeOfRec);
    printf(" \n total Records in page (blkftr) [%d]",tab_info->blkFctr);
    printf(" \n total Attributes in table [%d]",tab_info->rm_tbl_data->schema->numAttr);
    printf(" \n next available page and slot [%d:%d]",tab_info->firstFreeLoc.page,tab_info->firstFreeLoc.slot);
}