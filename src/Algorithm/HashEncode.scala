package Algorithm

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.math.BigInteger;
import java.security.MessageDigest;

object HashEncode{
    def main(args: Array[String]) {

//      println(hmacHash("dfer"))
//      println(md5Hash("dfer"))
      println( HashMD5("a13049c9f1874327b8a1257ee553f0a9") )

   }
    

  def HashHmac(key: String): Long = {
    val key = "a13049c9f1874327b8a1257ee553f0a9";
    val sk = new SecretKeySpec(key.getBytes("UTF-8"), "HmacMD5");
    val mac = Mac.getInstance("HmacMD5");
    mac.init(sk);

    val bi = new BigInteger(mac.doFinal(key.getBytes("UTF-8")));
    val result = bi.toString().substring(bi.toString().length() - 15).toLong

    result;
  }
  
  def BKDRHash( str:String) :Long ={
   val seed:Long  = 131 // 31 131 1313 13131 131313 etc..
   var hash:Long  = 0
   for(i <- 0 to str.length-1){
    hash = hash * seed + str.charAt(i)
    hash = hash.&("137438953471".toLong)        //0x1FFFFFFFFF              //固定一下长度
   }
   return hash 
}
    
  
   
  def HashMD5(s: String): Long = {
    val m = MessageDigest.getInstance("MD5");
    val b = s.getBytes("UTF-8");
    m.update(b, 0, b.length);
    
    val result = new BigInteger(m.digest()).toString();
   
    result.substring(result.length()-15).toLong;
  }
  


}