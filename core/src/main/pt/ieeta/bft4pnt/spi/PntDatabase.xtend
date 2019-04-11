package pt.ieeta.bft4pnt.spi

import java.util.concurrent.ConcurrentHashMap

class PntDatabase {
  static val db = new ConcurrentHashMap<String, PntDatabase>
  
  static def PntDatabase get(String name) { db.get(name) }
  static def void set(String name, StoreManager store, IFileManager files) { db.put(name, new PntDatabase(store, files)) }
  
  val StoreManager store
  val IFileManager files
  
  new (StoreManager store, IFileManager files) {
    this.store = store
    this.files = files
  }
  
  def StoreManager store() { store }
  def IFileManager files() { files }
}
