package pt.ieeta.bft4pnt.msg

class ReadResult {
  Error error = null
  Message msg = null
  
  new(String errorMsg) { this.error = new Error(Error.Code.INVALID, errorMsg) }
  new(Message msg) { this.msg = msg }
  
  def boolean hasError() { error !== null }
  
  def getError() { error }
  
  def getMsg() {
    if (error !== null)
      throw new RuntimeException("Cannot retrieve a result with errors!")
    return msg
  }
}