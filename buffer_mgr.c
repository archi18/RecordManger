#include "buffer_mgr.h"
#include "dberror.h"
#include "storage_mgr.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>


PIN_PAGE *firstPage=NULL;

RC updateBMPageHandle(PageNumber , char * ,BM_PageHandle *);

SM_FileHandle *sm_fileHandle;
BM_BufferPool bm_BufferPool;
BM_PageHandle bm_PageHandle;
SM_PageHandle ph;
int numReadIO;
int numWriteIO;
RC COD_RTN=0;

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
                  const int numPages, ReplacementStrategy strategy,
                  void *stratData){

    if(bm==NULL)
        return RC_BM_SPC_ALLOC_FAILED;

    sm_fileHandle = MAKE_SM_FILE_HANDLE();

    numReadIO = 0;
    numWriteIO =0;

    bm->pageFile = pageFileName;
    bm->numPages = numPages;
    bm->strategy = strategy;
    bm->mgmtData = stratData;

    if(firstPage==NULL)
        firstPage = createBufferOfSize(numPages,firstPage);
    if(firstPage!=NULL)
        printf("\n Buffer created successfully");

    displayBuffContent(firstPage);

    int COD_RTN = openPageFile(pageFileName,sm_fileHandle);
    printf("\n in side initBufferPool Result :: %s",sm_fileHandle->fileName);

    return COD_RTN;
}
RC shutdownBufferPool(BM_BufferPool *const bm){
    return RC_OK;
}
RC forceFlushPool(BM_BufferPool *const bm){
    return RC_OK;
}

// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page){
    return RC_OK;
}
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page){
    return RC_OK;
}
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page){
    return RC_OK;
}
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,
            const PageNumber pageNum){
    return RC_OK;
}

// Statistics Interface
PageNumber *getFrameContents (BM_BufferPool *const bm){
    return RC_OK;
}
bool *getDirtyFlags (BM_BufferPool *const bm){
    return RC_OK;
}
int *getFixCounts (BM_BufferPool *const bm){
    return RC_OK;
}
int getNumReadIO (BM_BufferPool *const bm){
    return RC_OK;
}
int getNumWriteIO (BM_BufferPool *const bm){
    return RC_OK;
}


RC updateBMPageHandle(PageNumber pageNum, char *data ,BM_PageHandle *bmPageHandle){
    bmPageHandle->pageNum = pageNum;
    bmPageHandle->data = data;
    return RC_OK;
}

PIN_PAGE* createBufferOfSize(int numPages,PIN_PAGE *head){
    for(int i=0;i<numPages;i++){
        firstPage= createFrame(head);
        head=firstPage;
    }
    return head;
}

PIN_PAGE* createFrame(PIN_PAGE *head){
    PIN_PAGE *tempPage;

    if(head==NULL){
        head = malloc(sizeof(PIN_PAGE));
        head->nxt_pin_page=NULL;
        head->data = NULL;
        head->pageNum = -1;
        head->isDirty = FALSE;
        head->fixCount = 0;
        printf("\n Head created ");
        return head;
    }
    tempPage=head;
    while(tempPage->nxt_pin_page!=NULL)
        tempPage=tempPage->nxt_pin_page;
    tempPage->nxt_pin_page= malloc(sizeof(PIN_PAGE));
    tempPage->nxt_pin_page->pageNum = -1;
    tempPage->nxt_pin_page->data = NULL;
    tempPage->nxt_pin_page->isDirty=FALSE;
    tempPage->nxt_pin_page->fixCount=0;
    tempPage->nxt_pin_page->nxt_pin_page=NULL;
    printf("\n page created ");
    return head;
}

PIN_PAGE* updatePageInfo(PIN_PAGE *page,char *pageData, boolean isDirtyFlag, int fixCount, int pageNum){
    page->data=pageData;
    page->isDirty=isDirtyFlag;
    page->fixCount=fixCount;
    page->pageNum=pageNum;

    return page;
}

RC displayBuffContent(PIN_PAGE *firstPage){
    PIN_PAGE *tempPage=firstPage;

    while(tempPage!=NULL){
        printf("\n The page num :: %d",tempPage->pageNum);
        printf("\n The page data :: %s",tempPage->data);
        tempPage=tempPage->nxt_pin_page;
    }
}

RC testReadBlock(){
    ph = (SM_PageHandle)malloc(PAGE_SIZE);
    readBlock(0,sm_fileHandle,ph);
    for(int i=0; i<PAGE_SIZE; i++)
        printf("\n content read %c",ph[i]);
}
