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
 
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 * Yue Ji updated on 9/18/2013
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
/**
 *
 * @author rl95
 */
public class Serial_HathiTrust {
    private PrintWriter hLogPrinter;

    public Serial_HathiTrust() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd.HHmmss");
        String separator = System.getProperty("file.separator");
        String nameSerial = "";
        try {
            nameSerial = "HathiTrust_Files" + separator + "yale_serials_" + sdf.format(new Date()) + ".tsv";
            File hLog = new File(nameSerial);
            FileWriter hLogWriter = new FileWriter(hLog);
            hLogPrinter = new PrintWriter(hLogWriter);
        } catch (IOException e) {
        System.err.println("Logging error creating new log: " + e);
        System.exit(99);
       }
        doQuery();
    }

    private void doQuery() {
        try {
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            Connection con =
            DriverManager.getConnection ("jdbc:oracle:thin:@[server name]",
                                    "[server username]", "[server password]");
            Statement stmt = con.createStatement();

            String strSQL = "Select Bib_Index.display_heading, Bib_Text.bib_id, Bib_Text.ISSN";
/* 008 position 28, start as 0, but SUBSTR start as 1, so is 29 here */
            strSQL = strSQL + ", (case SUBSTR(Bib_Text.field_008, 29, 1) ";
            strSQL = strSQL + "when 'f' then '1' ";
            strSQL = strSQL + "else '0' ";
            strSQL = strSQL + "end) Gov ";
            strSQL = strSQL + "from Bib_Master, Bib_Text, Bib_Index ";
            strSQL = strSQL + "where Bib_Master.Bib_Id = Bib_Text.Bib_Id ";
            strSQL = strSQL + "and Bib_Master.Bib_Id = Bib_Index.Bib_Id ";
            strSQL = strSQL + "and Bib_Index.Index_Code = '079A' ";
            strSQL = strSQL + "and Bib_Text.bib_format = 'as' ";
            strSQL = strSQL + "and Bib_Master.Suppress_In_OPAC = 'N' ";
           // strSQL = strSQL + "and Bib_Master.Suppress_In_OPAC = 'N' and Bib_Master.Bib_Id=12945";

            ResultSet rs = stmt.executeQuery(strSQL);

            long ctr = 0;
            String iSSN = "";

            hLogPrinter.println("OCLC # \t Bib ID  \t ISSN \t GovDocs");
            while (rs.next())
            {
                ++ctr;
                if (ctr%20000==0) System.out.println("RowCount:" + ctr + "  " + rs.getString(1));
                hLogPrinter.print(rs.getString(1));         // OCLC
                
                hLogPrinter.print("\t" + rs.getString(2));  // Bib ID
                
                iSSN = rs.getString(3);
                if (iSSN == null) iSSN = "";
          
                hLogPrinter.print("\t" + iSSN);  // ISSN
                
                hLogPrinter.println("\t" + rs.getString(4));  // GovDocs
            }
        }
        catch (Exception e) {
            System.err.println("Exception caught: " + e.getMessage());
        }
        hLogPrinter.flush();
        hLogPrinter.close();
    }

    /**
    * @param args the command line arguments
    */
    public static void main(String args[]) {
        new Serial_HathiTrust();
    }
}
