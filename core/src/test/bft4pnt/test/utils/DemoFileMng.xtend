package bft4pnt.test.utils

import pt.ieeta.bft4pnt.spi.IFileManager
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor
import java.io.RandomAccessFile
import java.io.FileNotFoundException

@FinalFieldsConstructor
class DemoFileMng implements IFileManager {
  val String lStore
  
  override exist(String record, int index) {
    if (getFile(record, index) !== null)
      return Status.YES
    
    // this demo doesn't implement PENDING
    
    return Status.NO
  }
  
  override getFile(String record, int index) {
    val fileName  = record.replace("/", "_").replace("+", "-")
    val path = lStore + "/" + fileName + "-" + index
    
    try {
      new RandomAccessFile(path, "r")
    } catch (FileNotFoundException ex) {
      ex.printStackTrace
      return null;
    }
  }
}