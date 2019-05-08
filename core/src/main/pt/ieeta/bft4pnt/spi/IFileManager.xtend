package pt.ieeta.bft4pnt.spi

import java.io.RandomAccessFile

interface IFileManager {
  enum Status { YES, NO, PENDING }
  
  def Status exist(String record, int index)
  def RandomAccessFile getFile(String record, int index)
}