import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.Graph.graphToGraphOps
import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import Algorithm._
import scala.collection.mutable.MutableList
import scala.Range
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.{Buffer,Set,Map}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import Algorithm._

object staticProp {
  

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);
    Logger.getLogger("hive").setLevel(Level.WARN);
    Logger.getLogger("parse").setLevel(Level.ERROR);

    //    require(args.length == 3)

    val conf = new SparkConf().setAppName("Graphx Test")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)
   
    
//    val transdata = hc.sql(s"select cross_dist_in, count(*) from tbl_common_his_trans where pdate=20160701 and trans_id='S22' group by cross_dist_in")
//    transdata.show 
 //val DisperseList = List("settle_tp","settle_cycle","block_id","trans_fwd_st","trans_rcv_st","sms_dms_conv_in","fee_in","cross_dist_in","orig_acpt_sdms_in","tfr_in_in","trans_md","source_region_cd","dest_region_cd","cups_card_in","cups_sig_card_in","card_class","card_attr","sti_in","trans_proc_in","acq_ins_id_cd","acq_ins_tp","fwd_ins_id_cd","fwd_ins_tp","rcv_ins_id_cd","rcv_ins_tp","iss_ins_id_cd","iss_ins_tp","related_ins_id_cd","related_ins_tp","acpt_ins_tp","auth_id_resp_cd","resp_cd1","resp_cd2","resp_cd3","resp_cd4","cu_trans_st","sti_takeout_in","trans_id","trans_tp","trans_chnl","card_media","card_media_proc_md","card_brand","expire_seg","trans_id_conv","settle_fwd_ins_id_cd","settle_rcv_ins_id_cd","trans_curr_cd","cdhd_fee_conv_rt","cdhd_fee_acct_curr_cd","exp_snd_chnl","confirm_exp_chnl","conn_md","msg_tp","msg_tp_conv","card_bin","related_card_bin","trans_proc_cd","trans_proc_cd_conv","conv_dt","mchnt_tp","pos_entry_md_cd","card_seq","pos_cond_cd","pos_cond_cd_conv","term_id","term_tp","mchnt_cd","rsn_cd","addn_pos_inf","orig_msg_tp","orig_msg_tp_conv","orig_sys_tra_no","orig_sys_tra_no_conv","related_trans_id","related_trans_chnl","orig_trans_id","orig_trans_id_conv","orig_trans_chnl","orig_card_media","orig_card_media_proc_md","spec_settle_in","settle_trans_id","spec_mcc_in","iss_ds_settle_in","acq_ds_settle_in","upd_in","exp_rsn_cd","pri_cycle_no","alt_cycle_no","corr_pri_cycle_no","corr_alt_cycle_no","disc_in","orig_disc_in","orig_disc_curr_cd","fwd_settle_conv_rt","rcv_settle_conv_rt","fwd_settle_curr_cd","rcv_settle_curr_cd","disc_cd","allot_cd","sp_mchnt_cd","acct_ins_id_cd","pay_in","exp_id","vou_in","orig_log_cd","related_log_cd","rec_upd_ts","rec_crt_ts","trans_media")
   
    val DisperseList = List("settle_tp","settle_cycle","block_id")
    //    val DisperseList = List("cross_dist_in","cups_card_in")
//    
    for(item <- DisperseList){
      val data =  hc.sql(s"select $item, count(*) from tbl_common_his_trans where pdate=20160701 and trans_id ='S22' group by $item")
      data.show 
    }
  }
  
  
  
  
  
  
}