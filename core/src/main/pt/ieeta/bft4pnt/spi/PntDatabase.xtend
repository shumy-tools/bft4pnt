package pt.ieeta.bft4pnt.spi

import java.util.concurrent.ConcurrentHashMap
import pt.ieeta.bft4pnt.msg.Quorum

class PntDatabase {
  static val db = new ConcurrentHashMap<String, PntDatabase>
  
  static def PntDatabase get(String name) { db.get(name) }
  static def void set(String name, IStoreManager store, IFileManager files) { db.put(name, new PntDatabase(store, files)) }
  
  val IStoreManager store
  val IFileManager files
  val IRecord quorum
  
  new (IStoreManager store, IFileManager files) {
    this.store = store
    this.files = files
    
    val localStore = store.local
    val qAlias = store.alias("quorum")
    quorum = localStore.getRecord(qAlias)
  }
  
  def IStoreManager store() { store }
  def IFileManager files() { files }
  
  synchronized def Quorum getCurrentQuorum() {
    //TODO: use thread local for performance?
    getQuorumAt(quorum.lastIndex)
  }
  
  synchronized def Quorum getQuorumAt(int index) {
    val qMsg = quorum.getCommit(index)
    if (qMsg === null)
      throw new RuntimeException('''No quorum at index: «index»''')
    
    val q = qMsg.data.get(Quorum)
    if (q.index !== index)
      throw new RuntimeException('''Incorrect quorum index: («q.index» != «index»)''')
    
    return q
  }
}
