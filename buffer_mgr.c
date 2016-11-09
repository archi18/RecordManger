#include "buffer_mgr.h"
#include "dberror.h"
#include "storage_mgr.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>


PIN_PAGE *firstPage=NULL;

RC updateBMPageHandle(PageNumber , char * ,BM_PageHandle *);
void printPageDataBF(char *);
SM_FileHandle *fh;
BM_BufferPool bm_BufferPool;
BM_PageHandle bm_PageHandle;
SM_PageHandle ph;
int numReadIO;
int numWriteIO;
int buffFrameCount=0;
int bufferFrameSize;
int *buffPageLookUpTbl;
BM_FrameStat frameStat;
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
                  const int numPages, ReplacementStrategy strategy,
                  void *stratData){

    if(bm==NULL)
        return RC_BM_SPC_ALLOC_FAILED;

    fh = MAKE_SM_FILE_HANDLE();
    bufferFrameSize = numPages;

    numReadIO = 0;
    numWriteIO =0;


    frameStat.frameContent_arr = (int *)malloc(sizeof(int) * numPages);
    frameStat.frameDirtyFlg_arr = (int *)malloc(sizeof(int) * numPages);
    frameStat.framefixCount_arr = (int *)malloc(sizeof(int) * numPages);
    frameStat.numReadIO = 0;
    frameStat.numWriteIO =0;

    for(int i=0; i<numPages; i++){
        frameStat.frameContent_arr[i] = -1;
        frameStat.frameDirtyFlg_arr[i] = 0;
        frameStat.framefixCount_arr[i] = 0;
    }


    bm->pageFile = pageFileName;
    bm->numPages = numPages;
    bm->strategy = strategy;
    bm->mgmtData = stratData;


    int frameLookUpTbl[bufferFrameSize];
    for(int i=0; i<bufferFrameSize; i++)
        frameLookUpTbl[i]=-1;


    buffPageLookUpTbl=frameLookUpTbl;



    firstPage=NULL;
    firstPage = createBufferOfSize(numPages,firstPage);
    if(firstPage!=NULL)
        printf("\n Buffer created successfully ");


    int rtnCod=openPageFile(pageFileName,fh);
    return rtnCod;
}
RC shutdownBufferPool(BM_BufferPool *const bm){
    PIN_PAGE *tempPage;

    int pageLoc[bufferFrameSize];
    returnPagePosition(firstPage,&pageLoc);
    buffPageLookUpTbl = &pageLoc;

    for(int i=0; i<bufferFrameSize; i++){
        if(pageLoc[i]!=-1){
            int indexFrame = isPagePresent(pageLoc[i]);
            if(indexFrame==-1)
                return RC_IVLD_PAGE_NUM;
            tempPage = getFrameFromLoc(firstPage,indexFrame);
            if(tempPage->fixCount ==0){
                if(tempPage->isDirty==TRUE){

                    if(RC_OK != writeBlock(tempPage->pageNum,fh,tempPage->data)){
                        return RC_WRITE_FAILED;
                    }
                    tempPage->isDirty=FALSE;
                    numWriteIO = numWriteIO + 1;
                    frameStat.numWriteIO = frameStat.numWriteIO +1;

                }
            }else{
                return RC_BUFFER_IN_USE;
            }
        }
    }
   // freeBM_Resources(firstPage);
    printf("shutdown success");
    return RC_OK;
}
RC forceFlushPool(BM_BufferPool *const bm){
    PIN_PAGE *tempPage;

    int pageLoc[bufferFrameSize];
    returnPagePosition(firstPage,&pageLoc);
    buffPageLookUpTbl = &pageLoc;
    printf("\n inside shutdown buffer");

    for(int i=0; i<bufferFrameSize; i++){
        if(pageLoc[i]!=-1){
            int indexFrame = isPagePresent(pageLoc[i]);
            if(indexFrame==-1)
                return RC_IVLD_PAGE_NUM;
            tempPage = getFrameFromLoc(firstPage,indexFrame);
            if(tempPage->fixCount ==0){
                if(tempPage->isDirty==TRUE){

                    if(RC_OK != writeBlock(tempPage->pageNum,fh,tempPage->data)){
                        return RC_WRITE_FAILED;
                    }
                    tempPage->isDirty=FALSE;
                    updatePageFrameStat(tempPage,&frameStat,0,-1);

                }
                numWriteIO = numWriteIO + 1;
                frameStat.numWriteIO = frameStat.numWriteIO +1;

            }
        }
    }

    printf("\n forceFlushPool success");
    return RC_OK;
}

// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page){
    PIN_PAGE *tempPage;

    int pageLoc[bufferFrameSize];
    returnPagePosition(firstPage,&pageLoc);
    buffPageLookUpTbl = &pageLoc;
    int indexFrame = isPagePresent(page->pageNum);

    if(indexFrame==-1)
        return RC_IVLD_PAGE_NUM;

    tempPage = getFrameFromLoc(firstPage,indexFrame);
    tempPage->isDirty = TRUE;
    updatePageFrameStat(tempPage,&frameStat,1,0);


    return RC_OK;
}
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page){
    PIN_PAGE *tempPage;
    if(page->pageNum < 0){
        printf(" \n page number null");
        return RC_INVLD_PAGE_NUM;
    }

    int pageLoc[bufferFrameSize];
    returnPagePosition(firstPage,&pageLoc);
    buffPageLookUpTbl = &pageLoc;
    int indexFrame = isPagePresent(page->pageNum);

    if(indexFrame==-1)
        return RC_IVLD_PAGE_NUM;

    tempPage = updateFixCountAtFrameLoc(firstPage,indexFrame,-1);
    updatePageFrameStat(tempPage,&frameStat,tempPage->isDirty,-1);
    page->data = tempPage->data;

    return RC_OK;
}
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page){

    PIN_PAGE *tempPage;

    int pageLoc[bufferFrameSize];
    returnPagePosition(firstPage,&pageLoc);
    buffPageLookUpTbl = &pageLoc;
    int indexFrame = isPagePresent(page->pageNum);

    if(indexFrame==-1)
        return RC_IVLD_PAGE_NUM;

    tempPage = getFrameFromLoc(firstPage,indexFrame);

    if(tempPage->fixCount ==0){
        if(tempPage->isDirty==TRUE){

            if(RC_OK != writeBlock(tempPage->pageNum,fh,tempPage->data)){
                return RC_WRITE_FAILED;
            }
            tempPage->isDirty=FALSE;
            numWriteIO = numWriteIO + 1;

        }
    }else{
        return RC_BUFFER_IN_USE;
    }

    printf("\n forcepage success");
    return RC_OK;
}
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,
            const PageNumber pageNum){
    ensureCapacity(pageNum+1,fh);

    PIN_PAGE *tempPage;

    int pageLoc[bufferFrameSize];
    returnPagePosition(firstPage,&pageLoc);
    buffPageLookUpTbl = &pageLoc;
    int noOfPageInBuff=0;

    for(int i=0; i<bufferFrameSize; i++){
        if(pageLoc[i]!= -1)
            noOfPageInBuff++;
    }

    int indexFrame = isPagePresent(pageNum);

    if(indexFrame != -1){
        printf("Page Hit");
        tempPage = updateFixCountAtFrameLoc(firstPage,indexFrame,1);
        page->data = tempPage->data;
        page->pageNum = pageNum;

        updatePageFrameStat(tempPage,&frameStat,tempPage->isDirty,1);
        if(bm->strategy==RS_LRU)
            firstPage = moveFrameCurtLocToEnd(firstPage,indexFrame);
        return RC_OK;
    }

    tempPage = getFrameFromLoc(firstPage,indexFrame);
    if(tempPage->fixCount > 0)
        firstPage=moveHeadToEnd(firstPage);

    indexFrame = getIndexPageByFIFO();

    if(indexFrame ==-1)
        return RC_UNKNOWN_ERROR;
    else{

        boolean isDirty = isPageDirty(firstPage,indexFrame);
        if(isDirty){
            tempPage = getFrameFromLoc(firstPage,indexFrame);

            if(RC_OK != writeBlock(tempPage->pageNum,fh,tempPage->data)){
                return RC_WRITE_FAILED;
            }
            frameStat.numWriteIO = frameStat.numWriteIO +1;

        }

        tempPage = getFrameFromLoc(firstPage,indexFrame);
        updateFrameStat(tempPage,&frameStat,pageNum);


        if (RC_OK != readBlock(pageNum, fh, tempPage->data)){
            return RC_READ_FAILED;
        }
        tempPage->fixCount =1;
        tempPage->isDirty = FALSE;
        page->data =tempPage->data;
        page->pageNum = pageNum;
      //  printPageDataBF(page->data);

        buffPageLookUpTbl[indexFrame]=pageNum;
        updatePinPageAtLoc(firstPage,indexFrame,pageNum,NULL);

        if(noOfPageInBuff>=3) {
            firstPage = moveHeadToEnd(firstPage);
            shiftPageLookTbl();
        }

        numReadIO = numWriteIO + 1;
        frameStat.numReadIO = frameStat.numReadIO + 1;

    }

    return RC_OK;
}

void printPageDataBF(char *pageData){
    printf("\n BufferMgr Prining page Data ==>");

    for(int i=0; i<PAGE_SIZE; i++){
        printf("%c",pageData[i]);
    }
    printf("\n exiting ");
    printf("\n exiting ");
}

// Statistics Interface
PageNumber *getFrameContents (BM_BufferPool *const bm){

    return frameStat.frameContent_arr;
}
bool *getDirtyFlags (BM_BufferPool *const bm){
    bool *dirtyFlag = (bool*)malloc(sizeof(bool) * bm->numPages);

    PIN_PAGE *tempPage = firstPage;
    int i=0;
    while(tempPage!=NULL){
        dirtyFlag[i]=frameStat.frameDirtyFlg_arr[i];
        tempPage=tempPage->nxt_pin_page;
        i++;
    }

    return dirtyFlag;
}
int *getFixCounts (BM_BufferPool *const bm){
    return frameStat.framefixCount_arr;
}
int getNumReadIO (BM_BufferPool *const bm){
    return frameStat.numReadIO;
}
int getNumWriteIO (BM_BufferPool *const bm){
    return numWriteIO;
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
        head->data = (char *) malloc(PAGE_SIZE*sizeof(char));
        head->pageNum = -1;
        head->isDirty = FALSE;
        head->fixCount = 0;
        return head;
    }
    tempPage=head;
    while(tempPage->nxt_pin_page!=NULL)
        tempPage=tempPage->nxt_pin_page;
    tempPage->nxt_pin_page= malloc(sizeof(PIN_PAGE));
    tempPage->nxt_pin_page->pageNum = -1;
    tempPage->nxt_pin_page->data = (char *) malloc(PAGE_SIZE*sizeof(char));
    tempPage->nxt_pin_page->isDirty=FALSE;
    tempPage->nxt_pin_page->fixCount=0;
    tempPage->nxt_pin_page->nxt_pin_page=NULL;
    return head;
}

PIN_PAGE* moveHeadToEnd(PIN_PAGE *head){
    PIN_PAGE *tempPage=head;
    PIN_PAGE *lastPage=head;

    while(lastPage->nxt_pin_page!=NULL)
        lastPage= lastPage->nxt_pin_page;

    head=head->nxt_pin_page;

    lastPage->nxt_pin_page = tempPage;
    lastPage->nxt_pin_page->nxt_pin_page = NULL;
    return head;
}

PIN_PAGE* moveFrameCurtLocToEnd(PIN_PAGE *head,int frameIndex){
    PIN_PAGE *tempPage=head;
    PIN_PAGE *lastPage=head;
    PIN_PAGE *prevPage;
    int counter=0;
    if(frameIndex==0)
        return moveHeadToEnd(head);
    if(frameIndex==(bufferFrameSize-1))
        return head;

    while(counter<frameIndex){
        prevPage=tempPage;
        tempPage=tempPage->nxt_pin_page;
        counter++;
    }

    while(lastPage->nxt_pin_page!=NULL)
        lastPage= lastPage->nxt_pin_page;

    prevPage->nxt_pin_page=tempPage->nxt_pin_page;

    lastPage->nxt_pin_page = tempPage;
    lastPage->nxt_pin_page->nxt_pin_page = NULL;
    return head;
}
PIN_PAGE* updatePageInfo(PIN_PAGE *page,char *pageData, boolean isDirtyFlag, int fixCount, int pageNum){
    page->data=pageData;
    page->isDirty=isDirtyFlag;
    page->fixCount=fixCount;
    page->pageNum=pageNum;

    return page;
}

PIN_PAGE* updatePinPageAtLoc(PIN_PAGE *head,int frameIndex,int pageNum, char *newData){
    PIN_PAGE *tempPage=head;
    int counter=0;
    while(counter<frameIndex){
        tempPage=tempPage->nxt_pin_page;
        counter++;
    }

    tempPage->pageNum= pageNum;

    return head;
}


void shiftPageLookTbl(){
    int firstFrame =buffPageLookUpTbl[0];
    for(int i=0; i<(bufferFrameSize-1);i++){
        buffPageLookUpTbl[i]=buffPageLookUpTbl[i+1];
    }
    buffPageLookUpTbl[bufferFrameSize-1]=firstFrame;
}

int getIndexPageByFIFO(){
    int indexFrame=-1;
    if(buffFrameCount<bufferFrameSize){
        for(int i=0; i<bufferFrameSize; i++){
            if(buffPageLookUpTbl[i]==-1){
                indexFrame=i;
                break;
            }
        }
    }
    if(indexFrame==-1)
        indexFrame =0;
    return indexFrame;
}

int isPagePresent(int pageNum){
    int frameIndex=-1;

    for(int i=0; i <bufferFrameSize; i++){
        if(buffPageLookUpTbl[i]==pageNum){
            frameIndex = i;
            break;
        }
    }
    return frameIndex;
}

int isPageDirty(PIN_PAGE * head,int frameIndex){
    PIN_PAGE *tempPage=head;
    int counter=0;
    while(counter<frameIndex){
        tempPage=tempPage->nxt_pin_page;
        counter++;
    }
    return tempPage->isDirty;
}

PIN_PAGE* getFrameFromLoc(PIN_PAGE * head, int frameIndex){
    PIN_PAGE *tempPage=head;
    int counter=0;
    while(counter<frameIndex){
        tempPage=tempPage->nxt_pin_page;
        counter++;
    }
    return tempPage;
}

PIN_PAGE* updateFixCountAtFrameLoc(PIN_PAGE *head,int frameIndex,int value){
    PIN_PAGE *tempPage=head;
    int counter=0;
    while(counter<frameIndex){
        tempPage=tempPage->nxt_pin_page;
        counter++;
    }
    tempPage->fixCount =  tempPage->fixCount + value;
    return tempPage;
}

RC displayBuffContent(PIN_PAGE *firstPage){
    PIN_PAGE *tempPage=firstPage;

    while(tempPage!=NULL){
        tempPage=tempPage->nxt_pin_page;
    }
}

int* returnPagePosition(PIN_PAGE *head,int *loc){
    PIN_PAGE *tempPage = head;
    int pageLoc[bufferFrameSize];
    int count=0;
    while(tempPage!=NULL){
        *loc=tempPage->pageNum;
        tempPage=tempPage->nxt_pin_page;
        loc++;
        count++;
    }
    return loc;
}

void displayFrameStat(BM_FrameStat *stat){
    for(int i=0; i<bufferFrameSize; i++){
        printf("\n Content :: %d",stat->frameContent_arr[i]);
    }
}

BM_FrameStat* updateFrameStat(PIN_PAGE *olDFrame,BM_FrameStat *frameStat,int newPageNum){
    int oldPageNum=olDFrame->pageNum;
    int oldPageIndex=-1;

    for(int i=0; i<bufferFrameSize; i++){
        if(frameStat->frameContent_arr[i]==oldPageNum) {
            oldPageIndex = i;
            break;
        }
    }
    frameStat->frameContent_arr[oldPageIndex] = newPageNum;
    frameStat->frameDirtyFlg_arr[oldPageIndex] = 0;
    frameStat->framefixCount_arr[oldPageIndex] = 0;
    return  frameStat;
}

void updatePageFrameStat(PIN_PAGE *frame,BM_FrameStat *frameStat,int flag,int value){
    int pageNum=frame->pageNum;
    int pageIndex=-1;
    for(int i=0; i<bufferFrameSize; i++){
        if(frameStat->frameContent_arr[i]==pageNum) {
            pageIndex = i;
            break;
        }
    }
    frameStat->frameDirtyFlg_arr[pageIndex] = flag;
    if(frameStat->framefixCount_arr[pageIndex] == 0 && value ==-1){
        frameStat->framefixCount_arr[pageIndex] = 0;
    }else {
        frameStat->framefixCount_arr[pageIndex] = frameStat->framefixCount_arr[pageIndex] + value;
    }
}

void displayDirtyFLgFrameStat(BM_FrameStat *stat){
    for(int i=0; i<bufferFrameSize; i++){
        printf("\n %d FixCount :: %d",stat->frameContent_arr[i],stat->frameDirtyFlg_arr[i]);
    }
}

void freeBM_Resources(PIN_PAGE *head){
    PIN_PAGE *tempPage=head;

    while(tempPage!=NULL){
        PIN_PAGE *curPage=tempPage;
        tempPage=tempPage->nxt_pin_page;
        free(curPage->data);
        free(curPage);
    }
}
