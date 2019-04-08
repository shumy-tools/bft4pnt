package pt.ieeta.bft4pnt.spi

import java.util.concurrent.ConcurrentHashMap

class PntDatabase {
  static val db = new ConcurrentHashMap<String, PntDatabase>
  
  static def PntDatabase get(String name) { db.get(name) }
  static def void set(String name, IStoreManager store, IFileManager files) { db.put(name, new PntDatabase(store, files)) }
  
  val IStoreManager store
  val IFileManager files
  
  new (IStoreManager store, IFileManager files) {
    this.store = store
    this.files = files
  }
  
  def IStoreManager store() { store }
  def IFileManager files() { files }
}
