/*
@author {
	Reona Cerajo
	Jeevika Sundarrajam
	Li
	Archi Dsouza
}
*/


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "storage_mgr.h"
#include "dberror.h"

void updateSmFileHandle(int , int ,SM_FileHandle *);

FILE *sm_file;
SM_FileHandle sm_fileHandle;
//typedef char* SM_PageHandle;

void initStorageManager (void){
    printf("\n <*****************Initialize storage manager********************\n ");
}

RC createPageFile (char *fileName){
    sm_file = fopen(fileName,"w+");
    char *appendEmptyBlock = (char*) malloc (PAGE_SIZE * sizeof(char));
    if (!sm_file){
        return RC_FILE_NOT_FOUND;
    }
    memset(appendEmptyBlock,'\0',PAGE_SIZE);
    size_t rate_in = fwrite (appendEmptyBlock, sizeof(char), PAGE_SIZE, sm_file);
    sm_fileHandle.fileName = fileName;
    updateSmFileHandle(ONE,FIRST_PAGE,&sm_fileHandle);
    fclose(sm_file);
    return RC_OK;
}


RC openPageFile (char *fileName, SM_FileHandle *fHandle){
    sm_file = fopen(fileName,"rb+");
    if (!sm_file){
        return RC_FILE_NOT_FOUND;
    }

    fseek(sm_file,ZERO,SEEK_END);

    int last_byte_loc = ftell(sm_file);
    int totalNumPages = (int) last_byte_loc / PAGE_SIZE;
    fHandle->fileName =fileName;
    updateSmFileHandle(totalNumPages,ZERO,fHandle);
    rewind(sm_file);
    return RC_OK;
}

/*
This method will update information in SM_FileHandle
@param totalNumPages : total no pages in file
		curPagePos : current page position
		SM_FileHandle : represents an open page fil
*/
void updateSmFileHandle(int totalNumPages, int curPagePos,SM_FileHandle *sm_fileHandle){
    sm_fileHandle->totalNumPages = totalNumPages;
    sm_fileHandle->curPagePos = curPagePos;
}

RC closePageFile (SM_FileHandle *fHandle){

    if(fclose(sm_file)==ZERO)
        return  RC_OK;
    else
        return RC_FILE_NOT_FOUND;

}
RC destroyPageFile (char *fileName){
    if(sm_file)
        fclose(sm_file);
    if(remove(fileName)!= 0)
        return RC_FILE_NOT_FOUND;
    else
        return RC_OK;
}

RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
    if(fHandle ==NULL || fHandle->totalNumPages < ONE)
        return RC_FILE_HANDLE_NOT_INIT;
    if(pageNum > fHandle->totalNumPages) {
        printf("\n 11 failed  here ");
        return RC_READ_NON_EXISTING_PAGE;
    }

    fseek(sm_file,PAGE_SIZE * pageNum, SEEK_SET);
    size_t ret_Read = fread(memPage, PAGE_SIZE, sizeof(char),sm_file);

    if(ret_Read <= ZERO) {
        printf("\n Ftell %d ",ftell(sm_file));

        printf("\n 22 failed here %s ",memPage);
        return RC_READ_NON_EXISTING_PAGE;

    }
    updateSmFileHandle(fHandle->totalNumPages,pageNum,fHandle);
    return RC_OK;
}

RC getBlockPos (SM_FileHandle *fHandle){
    if(fHandle==NULL || fHandle->curPagePos <=ZERO)
        return RC_FILE_HANDLE_NOT_INIT;
    else
        return  fHandle->curPagePos;
}

RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    if(fHandle->totalNumPages <=ZERO)
        return RC_FILE_HANDLE_NOT_INIT;
    if(sm_file<=ZERO)
        return RC_FILE_NOT_FOUND;
    rewind(sm_file);
    // memPage =  malloc(sizeof(char)*PAGE_SIZE);
    size_t ret_Read=fread(memPage, PAGE_SIZE, sizeof(char),sm_file);
    if(ret_Read <= ZERO)
        return RC_READ_NON_EXISTING_PAGE;
    updateSmFileHandle(fHandle->totalNumPages, FIRST_PAGE,fHandle);
    return RC_OK;
}

RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if(fHandle->totalNumPages < ONE)
        return RC_FILE_HANDLE_NOT_INIT;
    if(sm_file==NULL)
        return RC_FILE_NOT_FOUND;
    fseek(sm_file,(fHandle->totalNumPages-PAGE_OFFSET)*PAGE_SIZE, SEEK_SET);
   // memPage = (char*)  malloc(sizeof(char)*PAGE_SIZE);
    size_t ret_Read=fread(memPage, PAGE_SIZE, sizeof(char),sm_file);
    if(ret_Read <= 0)
        return RC_READ_FAILED;
    updateSmFileHandle(fHandle->totalNumPages, fHandle->totalNumPages,fHandle);
    return RC_OK;
}

RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    if(fHandle->totalNumPages <=ZERO)
        return RC_FILE_HANDLE_NOT_INIT;
    if(sm_file<=ZERO)
        return RC_FILE_NOT_FOUND;
    int currentPagePos = fHandle->curPagePos;
  //  memPage = (char*) malloc(sizeof(char)*PAGE_SIZE);
    fseek(sm_file,(PAGE_SIZE * currentPagePos ),SEEK_SET);
    size_t ret_Read=fread(memPage, PAGE_SIZE, sizeof(char),sm_file);
    if(ret_Read <= ZERO)
        return RC_READ_NON_EXISTING_PAGE;
    updateSmFileHandle(fHandle->totalNumPages, currentPagePos,fHandle);
    return RC_OK;
}


RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (fHandle->totalNumPages < ONE)
        return RC_FILE_HANDLE_NOT_INIT;
    if (sm_file == NULL)
        return RC_FILE_NOT_FOUND;
    if (fHandle->curPagePos == FIRST_PAGE)
        return RC_READ_NON_EXISTING_PAGE;

    int prev_Page = fHandle->curPagePos - ONE;
    fseek(sm_file,(PAGE_SIZE*prev_Page),SEEK_SET);
 //   memPage = (char*) malloc(sizeof(char) * PAGE_SIZE);
    size_t ret_Read = fread(memPage, PAGE_SIZE, sizeof(char),sm_file);
    if (ret_Read <= ZERO)
        return RC_READ_FAILED;
    updateSmFileHandle(fHandle->totalNumPages, (fHandle->curPagePos - ONE), fHandle);

    return RC_OK;
}


RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (fHandle->totalNumPages < FIRST_PAGE)
        return RC_FILE_HANDLE_NOT_INIT;
    if (sm_file == NULL)
        return RC_FILE_NOT_FOUND;
    if (fHandle->curPagePos == (fHandle->totalNumPages-ONE))
        return RC_READ_NON_EXISTING_PAGE;

    int next_Page = fHandle->curPagePos + 1;
    fseek(sm_file, (PAGE_SIZE * next_Page), SEEK_SET);
   // memPage = (char*) malloc(sizeof(char) * PAGE_SIZE);
    size_t ret_Read = fread(memPage, PAGE_SIZE, sizeof(char),sm_file);
    if (ret_Read <= ZERO)
        return RC_READ_FAILED;
    updateSmFileHandle(fHandle->totalNumPages, next_Page, fHandle);

    return RC_OK;
}

RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (pageNum < 0 || fHandle == NULL) {
        return RC_WRITE_FAILED;
    }
    char *buff;
    int shiftPageBlockSize = sizeof(char) * (PAGE_SIZE * (fHandle->totalNumPages - pageNum));
    buff = (char*) malloc(shiftPageBlockSize);
    fseek (sm_file, pageNum * PAGE_SIZE, SEEK_SET);
    fread (buff, shiftPageBlockSize,1, sm_file);
    fseek (sm_file, pageNum * PAGE_SIZE, SEEK_SET);
    size_t  rate_out=fwrite (memPage, sizeof(char),PAGE_SIZE,sm_file);
    fwrite (buff,  sizeof(char),shiftPageBlockSize, sm_file);
    if (rate_out >= PAGE_SIZE) {
        updateSmFileHandle((fHandle->totalNumPages + ONE),pageNum,fHandle);
        return RC_OK;
    }else {
        return RC_WRITE_FAILED;
    }

}
RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    int pageNum = fHandle->curPagePos;
    if (pageNum < FIRST_PAGE || sm_file == NULL) {
        return RC_WRITE_FAILED;
    }
    char *buff;
    int shiftPageBlockSize = sizeof(char) * (PAGE_SIZE * (fHandle->totalNumPages - pageNum));
    buff = (char*) malloc(shiftPageBlockSize);
    fseek (sm_file, pageNum * PAGE_SIZE, SEEK_SET);
    fread (buff,  shiftPageBlockSize,1, sm_file);
    fseek (sm_file, pageNum * PAGE_SIZE, SEEK_SET);
    size_t  rate_out=fwrite (memPage, sizeof(char),PAGE_SIZE,sm_file);
    fwrite (buff,  sizeof(char),shiftPageBlockSize, sm_file);

    if (rate_out >= PAGE_SIZE) {
        updateSmFileHandle((fHandle->totalNumPages + ONE),(pageNum),fHandle);
        return RC_OK;
    }else {
        return RC_WRITE_FAILED;
    }
}
RC appendEmptyBlock(SM_FileHandle *fHandle) {
    char * appendEmptyBlock;
    appendEmptyBlock = (char *) malloc (PAGE_SIZE * sizeof(char));
    memset (appendEmptyBlock, '\0', PAGE_SIZE);
    int writesize = ZERO;
    fseek(sm_file,0,SEEK_END);
    writesize = fwrite (appendEmptyBlock, sizeof(char), PAGE_SIZE, sm_file);
    if (writesize >= PAGE_SIZE) {
        updateSmFileHandle((fHandle->totalNumPages + ONE),(fHandle->totalNumPages),fHandle);
        return RC_OK;
    }else {
        return RC_WRITE_FAILED;
    }
}
RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) {
    int pageNum = fHandle->totalNumPages;
    int writesize = ZERO;
    int extendpagesize = ZERO;
    char *write;
    if (numberOfPages > pageNum) {
        int incresePageSize=(numberOfPages - pageNum);
        extendpagesize =incresePageSize  * PAGE_SIZE;
        write = (char *) malloc(PAGE_SIZE * sizeof(char));
        memset(write, '\0', extendpagesize);
        writesize = fwrite(write, sizeof(char), extendpagesize, sm_file);
        if (writesize >= extendpagesize) {
            updateSmFileHandle((fHandle->totalNumPages + incresePageSize), (fHandle->totalNumPages-ONE + incresePageSize), fHandle);
            return RC_OK;
        } else {
            return RC_WRITE_FAILED;
        }
    }
    return RC_OK;
}