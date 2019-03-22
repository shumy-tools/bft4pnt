package pt.ieeta.bft4pnt.crypto

import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor

@FinalFieldsConstructor
class ArraySlice {
  public val byte[] data
  public val int offset
  public val int length
}