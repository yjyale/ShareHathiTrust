/*
 *  Copyright (c) 2013-2015 Yale University. All rights reserved.
 *
 *  THIS SOFTWARE IS PROVIDED "AS IS," AND ANY EXPRESS OR IMPLIED
 *  WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 *  MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE, ARE EXPRESSLY
 *  DISCLAIMED. IN NO EVENT SHALL YALE UNIVERSITY OR ITS EMPLOYEES BE
 *  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED, THE COSTS OF
 *  PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA OR
 *  PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 *  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 *  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *  SOFTWARE, EVEN IF ADVISED IN ADVANCE OF THE POSSIBILITY OF SUCH
 *  DAMAGE.
 *
 *  Redistribution and use of this software in source or binary forms,
 *  with or without modification, are permitted, provided that the
 *  following conditions are met:
 *
 *  1. Any redistribution must include the above copyright notice and
 *  disclaimer and this list of conditions in any related documentation
 *  and, if feasible, in the redistributed software.
 *
 *  2. Any redistribution must include the acknowledgment, "This product
 *  includes software developed by Yale University," in any related
 *  documentation and, if feasible, in the redistributed software.
 *
 *  3. The names "Yale" and "Yale University" must not be used to endorse
 *  or promote products derived from this software.
 */
 
/* Hathi Trust extracts SPM, MPM.
 * Yue Ji 9/18/2013
 * Yue Ji updated on 8/13/2014 for changing file naming conventions to <institution URL domain>_<type of file>_<date>.tsv. For example:
   1. Single-part monographs: yale_single-part_20131101.122559.tsv
   2. Multi-part monographs: yale_multi-part_20131101.122559.tsv
   3. Serials: yale_serials_20131101.122559.tsv
 */

import java.io.*;
import java.net.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;

public class SPM_MPM_HathiTrust {
   
    public SPM_MPM_HathiTrust() { 
        doQuery();
    }

    private void doQuery() {
        PreparedStatement pstmt866 = null, pstmtMICount = null, pstmtCount = null, pstmtSPM = null, pstmtIfMfhd = null, pstmt = null;
        PreparedStatement pstmtMPM = null, pstmtItem = null, pstmtMfhd = null, pstmtIfMfhdItem = null, pstmtMfhdItem = null;
        ResultSet rs866 = null, rsMICount = null, rsCount = null, rsSPM = null, rsMPM = null, rsMfhd = null;
        ResultSet rsMfhdItem = null, rsItem = null, rsMaxBid = null, rsIfMfhd = null, rsIfMfhdItem = null, rs = null;
        String sql866 = "", sqlMICount = "", sqlCount = "", sqlSPM = "", sqlMPM = "", sqlMfhd = "", itemenum = "", chron = "", strSQL = "";
        String bibid = "", mfhdid = "", itemid = "", itemStatus = "", iEnum = "", iChron = "", sqlIfMfhd = "";
        String nameSPM = "", nameMPM = "", sqlItem = "", sqlMfhdItem = "", sqlIfMfhdItem = "", sqlMaxBid = "";
        String separator = System.getProperty("file.separator");
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd.HHmmss");
        PrintWriter hLogPrinter = null, hLogPrinter_MPM = null, hLogPrinter_noMfhd = null;
        File hLog = null, hLog_MPM = null, hLog_noMfhd = null;
        FileWriter hLogWriter = null, hLogWriter_MPM = null, hLogWriter_noMfhd = null;
        int iCount = 0, startBid = 0, endBid = 0, maxBid = 0;
        int iMfhd = 0, itemCount = 0, iPrint = 0;
        long ctr = 0, ctrMPM = 0;
        boolean isSPM = false, isMPM = false, hasItem = false;
                 
        try {
// bibid file for the bibs without any holdings attached.
            hLog_noMfhd = new File("Bibids_No_Mfhd_Attached.txt");
            hLogWriter_noMfhd = new FileWriter(hLog_noMfhd);
            hLogPrinter_noMfhd = new PrintWriter(hLogWriter_noMfhd);
            hLogPrinter_noMfhd.println("Bibids with no mfhd record attached. These bibids are excluded from the sending files.");
            
// SPM(Single-Part Monographs) file.
            nameSPM = "HathiTrust_Files" + separator + "yale_single-part_" + sdf.format(new Date()) + ".tsv";
            nameMPM = "HathiTrust_Files" + separator + "yale_multi-part_" + sdf.format(new Date()) + ".tsv";
           
            hLog = new File(nameSPM);
            hLogWriter = new FileWriter(hLog);
            hLogPrinter = new PrintWriter(hLogWriter);
            hLogPrinter.println("OCLC # \t Bib ID  \t Holding Status \t Condition \t GovDocs");
            
// MPM(Multi-Part Monographs) file.            
            hLog_MPM = new File(nameMPM);
            hLogWriter_MPM = new FileWriter(hLog_MPM);
            hLogPrinter_MPM = new PrintWriter(hLogWriter_MPM);
            hLogPrinter_MPM.println("OCLC # \t Bib ID \t Holding Status \t Condition \t Enum/Chron \t GovDocs");
            
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            Connection con =
            DriverManager.getConnection ("jdbc:oracle:thin:@[server name]",
                                    "[server username]", "[server password]");
            Statement stmtMaxBid = con.createStatement();
            
            sqlIfMfhdItem = "select count(*) as itemCount from mfhd_item where mfhd_id = ?";
            
            sqlIfMfhd = "select mfhd_id from bib_mfhd where bib_id = ?";
            
            sqlMaxBid = "select max(bib_id) as mbid from bib_text";
                               
            sql866 = "select yaledb.GetMFHDtag(Bib_Mfhd.mfhd_id,'866') as tag866 "
                   + "from Bib_Mfhd, Mfhd_Data "
                   + "where Bib_Mfhd.bib_id = ? "
                   + "and Bib_Mfhd.mfhd_id = Mfhd_Data.mfhd_id";
            
            sqlMICount = "Select Mfhd_Item.mfhd_id, count(*) as itemCount "
                     + "from Bib_Mfhd, Mfhd_Item, Item "
                     + "where Bib_Mfhd.bib_id = ? "
                     + "and Bib_Mfhd.mfhd_id = Mfhd_Item.mfhd_id "
                     + "and Mfhd_Item.item_id = Item.item_id "
                     + "group by Mfhd_Item.mfhd_id";
           
            sqlCount = "Select distinct Mfhd_Item.mfhd_id, Mfhd_Item.item_enum, Mfhd_Item.chron "
                     + "from Bib_Mfhd, Mfhd_Item "
                     + "where Bib_Mfhd.bib_id = ? "
                     + "and Bib_Mfhd.mfhd_id = Mfhd_Item.mfhd_id ";
                                                
            sqlSPM = "Select Item.item_id"
                   + ", (case "
                   + "when item.perm_location = 573 then 'WD' "
                   + "else 'CH' end) ItemStatus "
                   + "from Bib_Item"
                   + ", Item "
                   + "where Bib_Item.bib_id = ? " 
                   + "and Bib_Item.item_id = Item.item_id";
                       
            sqlMPM = "Select Item.item_id"
                   + ", (case "
                   + "when item.perm_location = 573 then 'WD' "
                   + "else 'CH' end) ItemStatus"
                   + ", Mfhd_Item.item_enum"
                   + ", Mfhd_Item.chron "
                   + "from Bib_Mfhd"
                   + ", Mfhd_Item"
                   + ", Item "
                   + "where Bib_Mfhd.bib_id = ? "
                   + "and Bib_Mfhd.Mfhd_id = Mfhd_Item.Mfhd_id "
                   + "and Mfhd_Item.item_id = Item.item_id";
        
            sqlItem = "select (case "
                    + "when Item_Status.item_status in (12,13,14,15,16) then 'LM' "
                    + "when Item_Status.item_status = 17 then 'WD' "
                    + "else 'CH' end) ItemStatus "
                    + "from item_status "
                    + "where item_id = ? "
                    + "and item_status_date = (select max(item_status_date) from item_status "
                    + "where item_id = ?)";

            sqlMfhd = "Select Mfhd_master.location_id, Mfhd_master.display_call_no, Mfhd_master.mfhd_id "
                    + "from Bib_Mfhd, Mfhd_master "
                    + "where Bib_Mfhd.bib_id = ? "
                    + "and Bib_Mfhd.mfhd_id = Mfhd_master.mfhd_id";
            
            sqlMfhdItem = "Select * "
                        + "from Mfhd_Item, Mfhd_master "
                        + "where Mfhd_master.mfhd_id = ? "
                        + "and Mfhd_master.mfhd_id = Mfhd_Item.mfhd_id";
            
            strSQL = "Select Bib_Index.display_heading"
              + ", Bib_Text.bib_id"
/* 008 position 28, start as 0, but SUBSTR start as 1, so is 29 here */
              + ", (case SUBSTR(Bib_Text.field_008, 29, 1) "
              + "when 'f' then '1' "
              + "else '0' end) Gov "
              + "from Bib_Master"
              + ", Bib_Text"
              + ", Bib_Index "
              + "where Bib_Master.Suppress_In_OPAC = 'N' "
              + "and Bib_Text.bib_format = 'am' "
/* 008 position 23, start as 0, but SUBSTR start as 1, so is 24 here */
              + "and SUBSTR(Bib_Text.field_008, 24, 1) in ('d',' ') "
              + "and Bib_Index.Index_Code= '079A' " 
              + "and Bib_Master.Bib_Id = Bib_Text.Bib_Id "
              + "and Bib_Text.Bib_Id = Bib_Index.Bib_Id "
              + "and Bib_Text.Bib_Id between ? and ? ";
           //   + " and Bib_Text.Bib_Id = Bib_Index.Bib_Id and rownum <20000";
           //   + " and Bib_Text.Bib_Id in (6506)";

            pstmt = con.prepareStatement(strSQL);
            pstmt866 = con.prepareStatement(sql866);
            pstmtMICount = con.prepareStatement(sqlMICount);
            pstmtCount = con.prepareStatement(sqlCount);
            pstmtSPM = con.prepareStatement(sqlSPM);
            pstmtMPM = con.prepareStatement(sqlMPM);
            pstmtItem = con.prepareStatement(sqlItem);
            pstmtIfMfhd = con.prepareStatement(sqlIfMfhd);
          //  pstmtMfhd = con.prepareStatement(sqlMfhd,ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
            pstmtMfhd = con.prepareStatement(sqlMfhd);
            pstmtMfhdItem = con.prepareStatement(sqlMfhdItem);
            pstmtIfMfhdItem = con.prepareStatement(sqlIfMfhdItem);
            
            rsMaxBid = stmtMaxBid.executeQuery(sqlMaxBid);
            if (rsMaxBid.next()) {
               maxBid = rsMaxBid.getInt("mbid");
            }

            while (startBid <= maxBid) {
                endBid = startBid + 1000000;
       
                if (startBid > 0) {
                  hLogPrinter.flush();
                  hLogPrinter.close();
                
                  nameSPM = "HathiTrust_Files" + separator + "yale_single-part_" + sdf.format(new Date()) + ".tsv";
                  hLog = new File(nameSPM);
                  hLogWriter = new FileWriter(hLog);
                  hLogPrinter = new PrintWriter(hLogWriter);
                  hLogPrinter.println("OCLC # \t Bib ID  \t Holding Status \t Condition \t GovDocs");
              }
                             
              if (startBid > 0) {
                  hLogPrinter_MPM.flush();
                  hLogPrinter_MPM.close();
             
                  nameMPM = "HathiTrust_Files" + separator + "yale_multi-part_" + sdf.format(new Date()) + ".tsv";
                  hLog_MPM = new File(nameMPM);
                  hLogWriter_MPM = new FileWriter(hLog_MPM);
                  hLogPrinter_MPM = new PrintWriter(hLogWriter_MPM);
                  hLogPrinter_MPM.println("OCLC # \t Bib ID \t Holding Status \t Condition \t Enum/Chron \t GovDocs");
              }
               
              pstmt = con.prepareStatement(strSQL);
              pstmt.setInt(1, startBid);
              pstmt.setInt(2, endBid); 
              rs = pstmt.executeQuery();
   
              while (rs.next()) {
                bibid = rs.getString("bib_id");
            
                isSPM = isMPM = hasItem = false;
// Check if the bib has the mfhd attached.              
                iMfhd = 0;
                pstmtIfMfhd.setString(1, bibid);  
                rsIfMfhd = pstmtIfMfhd.executeQuery();
                while (rsIfMfhd.next()) {
                    iMfhd++;
                }
                if (iMfhd == 0) {
                    rsIfMfhd = null;
                    hLogPrinter_noMfhd.println(bibid); 
                    continue;
                }
                pstmt866.setString(1, bibid);
                rs866 = pstmt866.executeQuery();    
                rs866.next();
                if (rs866.getString(1) != null) {  
                    isMPM = true;
                }
                else { 
                    pstmtMICount.setString(1, bibid);
                    rsMICount = pstmtMICount.executeQuery(); 
                    while (rsMICount.next()) { 
                      if (rsMICount.getInt("itemCount") > 1) {
                         isMPM = true;
                         isSPM = false;
                         break;
                      }
                      else {
                        isSPM = true;
                        isMPM = false;
                      }
                    }
                   
                    if ((!isSPM) && (!isMPM)) {
                        isSPM = true;
                        isMPM = false;
                    }
       
                    if (isMPM) {
                    itemCount = iPrint = 0;
                    itemenum = chron = "";
                    pstmtCount.setString(1, bibid);
                    rsCount = pstmtCount.executeQuery(); 
                    while (rsCount.next()) { 
                        hasItem = true;
                        itemenum = rsCount.getString("item_enum");
                        chron = rsCount.getString("chron"); 
 // Check if item_enum has CD or DVD.                       
                        if ((itemenum != "") && (itemenum != null)) { 
                            itemenum = itemenum.toUpperCase();
                            if ((itemenum.indexOf("CD") < 0)
                                && (itemenum.indexOf("DVD") < 0)
                                && (itemenum.indexOf("ANSWER") < 0)
                                && (itemenum.indexOf("MAP") < 0)
                                && (itemenum.indexOf("PLAN") < 0)
                                && (itemenum.indexOf("CARTE") < 0)
                                && (itemenum.indexOf("PORTFOLIO") < 0)) {
                                iPrint++;
                            }
                        }
  // item_enum doesn't have CD or DVD. Check if item chron has CD or DVD. 
                        else { 
                          if ((chron != "") && (chron != null)) {
                            chron = chron.toUpperCase();
                            if ((chron.indexOf("CD") < 0)
                                && (chron.indexOf("DVD") < 0)
                                && (chron.indexOf("ANSWER") < 0)
                                && (chron.indexOf("MAP") < 0)
                                && (chron.indexOf("PLAN") < 0)
                                && (chron.indexOf("CARTE") < 0)
                                && (chron.indexOf("PORTFOLIO") < 0)) {
                                iPrint++;
                            }
                        }  
                        }
                         
                        if (iPrint > 1) {
                            isMPM = true;
                            isSPM = false;
                            break;
                        }
                        else {
                            isSPM = true;
                        } 
                        itemCount++; 
                    } 
                    if (!hasItem)
                        isSPM = true; 
                }
                }
                iCount = 0;
       
                if (isSPM) {
                  if ((ctr%100000 == 0) && (ctr >= 100000))
                    System.out.println("SPM RowCount:" + ctr + "  bib_id=" + rs.getString(2));  
                  
                  pstmtSPM.setString(1, bibid);
                  rsSPM = pstmtSPM.executeQuery(); 
                  while (rsSPM.next()) {
                    iCount++;
                    ++ctr;
                   
                    hLogPrinter.print(rs.getString("display_heading"));         // OCLC
                
                    hLogPrinter.print("\t" + bibid);  // Bib ID
                
                    itemid = rsSPM.getString("item_id");
                    itemStatus = rsSPM.getString("ItemStatus");
                    if (itemStatus.equals("CH")) {
                        pstmtItem.setString(1, itemid);
                        pstmtItem.setString(2, itemid);
                        rsItem = pstmtItem.executeQuery(); 
                        if (rsItem.next()) {
                            itemStatus = rsItem.getString("ItemStatus");
                        } 
                    }
                    hLogPrinter.print("\t" + itemStatus);  // Item status
                
                    hLogPrinter.print("\t");  // Condition, leave blank
    
                    hLogPrinter.println("\t" + rs.getString("Gov"));  // GovDocs
                  }
                }
                 
                if (isMPM) {
                  if ((ctrMPM%100000 == 0) && (ctrMPM >= 100000))
                    System.out.println("                MPM RowCount:" + ctrMPM + "  bib_id=" + rs.getString(2));
                
                  pstmtMPM.setString(1, bibid);
                  rsMPM = pstmtMPM.executeQuery(); 
                  while (rsMPM.next()) {
                    iCount++;
                    ++ctrMPM;
                   
                    hLogPrinter_MPM.print(rs.getString("display_heading"));         // OCLC
              
                    hLogPrinter_MPM.print("\t" + bibid);  // Bib ID
                
                    itemid = rsMPM.getString("item_id");
                    itemStatus = rsMPM.getString("ItemStatus");
                    if (itemStatus.equals("CH")) {
                        pstmtItem.setString(1, itemid);
                        pstmtItem.setString(2, itemid);
                        rsItem = pstmtItem.executeQuery(); 
                        if (rsItem.next()) {
                            itemStatus = rsItem.getString("ItemStatus");
                        } 
                    }
                    hLogPrinter_MPM.print("\t" + itemStatus);  // Item status
                
                    hLogPrinter_MPM.print("\t");  // Condition, leave blank
                
                    iEnum = rsMPM.getString("item_enum");
                    iChron = rsMPM.getString("chron");
                    if (iEnum == null) iEnum = "";
                    if (iChron == null) iChron = "";
                    hLogPrinter_MPM.print("\t" + iEnum + " " + iChron);  // Item enum, chron
                
                    hLogPrinter_MPM.println("\t" + rs.getString("Gov"));  // GovDocs
                  }
                }
  
// No items, look up mfhd to find item status. 
                if (iCount < iMfhd) { 
                    rsMfhd = null;
                    pstmtMfhd.setString(1, bibid); 
                    rsMfhd = pstmtMfhd.executeQuery();
                    while (rsMfhd.next()) {
                        mfhdid = rsMfhd.getString("mfhd_id");
                        pstmtMfhdItem.setString(1, mfhdid); 
                        rsMfhdItem = pstmtMfhdItem.executeQuery(); 
                        if (rsMfhdItem.next()) 
                            continue;
            
                        if (isSPM) {  
                            ++ctr;      
// OCLC number
                            hLogPrinter.print(rs.getString("display_heading"));         
// Bib ID                
                            hLogPrinter.print("\t" + bibid);  
// Item status
                            if ((rsMfhd.getString("location_id").equals("573")) 
                                || ((rsMfhd.getString("display_call_no") != null)
                                && (rsMfhd.getString("display_call_no").toLowerCase().indexOf("withdrawn") >= 0))) {
                                hLogPrinter.print("\tWD"); 
                            }
                            else
                                hLogPrinter.print("\tCH");
                            
                            hLogPrinter.print("\t");  // Condition, leave blank
                            
                            hLogPrinter.println("\t" + rs.getString("Gov"));  // GovDocs 
                        }
                        else { 
                            ++ctrMPM;
// OCLC number                        
                            hLogPrinter_MPM.print(rs.getString("display_heading"));         
// Bib ID                 
                            hLogPrinter_MPM.print("\t" + bibid);  
// Item status 
                            if ((rsMfhd.getString("location_id").equals("573")) 
                                || ((rsMfhd.getString("display_call_no") != null)
                                && (rsMfhd.getString("display_call_no").toLowerCase().indexOf("withdrawn") >= 0))) {
                                hLogPrinter_MPM.print("\tWD"); 
                            }
                            else
                                hLogPrinter_MPM.print("\tCH"); 
               
                            hLogPrinter_MPM.print("\t");  // Condition, leave blank
                
                            hLogPrinter_MPM.print("\t");  // Item enum, chron
                
                            hLogPrinter_MPM.println("\t" + rs.getString("Gov"));  // GovDocs
                        }   
                    }
                } 
                rsIfMfhd = null;
                rs866 = null;
                rsCount = null;
                rsMICount = null;
                rsSPM = null;
                rsMPM = null;
                rsItem = null;
                rsMfhd = null;
                rsMfhdItem = null;
              }
              rs = null;
              pstmt.close();
              startBid = endBid + 1; 
            }       
           
            hLogPrinter.flush();
            hLogPrinter.close();
            hLogPrinter_MPM.flush();
            hLogPrinter_MPM.close();
            hLogPrinter_noMfhd.flush();
            hLogPrinter_noMfhd.close();
                    
            rs = null;
            rsIfMfhdItem = null;
            rsIfMfhd = null;
            rs866 = null;
            rsMICount = null;
            rsCount = null;
            rsSPM = null;
            rsMPM = null;
            rsItem = null;
            rsMfhd = null;
            rsMfhdItem = null;
            rsMaxBid = null;
            
            pstmtIfMfhdItem.close();
            pstmt.close();
            pstmtIfMfhd.close();
            pstmt866.close();
            pstmtMICount.close();
            pstmtCount.close();
            pstmtSPM.close();
            pstmtMPM.close();
            pstmtItem.close();
            pstmtMfhd.close();
            pstmtMfhdItem.close();
            stmtMaxBid.close();
            con.close();
        
            System.out.println("Single Part Total RowCount: " + ctr);
            System.out.println("Multi Part Total RowCount: " + ctrMPM);
            System.out.println("The program is finished!");
        }
        catch (Exception e) {
            System.err.println("Exception caught: " + e.getMessage());
        }
    }
    /**
    * @param args the command line arguments
    */
    public static void main(String args[]) {
        new SPM_MPM_HathiTrust();
    }
}
