using MySql.Data.MySqlClient;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

using Newtonsoft.Json; //library package from nuget
using Newtonsoft.Json.Linq;
using System.Threading;
using System.Collections.Specialized;
using System.Globalization;

namespace Payment_job_scheduling_PG
{
    class Program
    {
        static StringBuilder sb = new StringBuilder();

        static string createText = null;
        static string createText_temp = null;
        static string information = null;
        static string path = @"c:\Log_folder_payment_icc_job\";
        static string file_log_name = null;
        static string full_path_file = null;

        static string cs_mysql = @"server=192.168.177.220;userid=root;password=Password@123;database=valsys_prod";
        static string cs_oracle = "User Id=mnc_subscribe;Password=mncsubsd3v;Data Source=192.168.177.101:1521/MNCSV";

        static string mysql_max_row_trx_table = "MAX_ROW_TRX"; 
        static string mysql_pg_trx_mirror_table = "PG_TRX_PAYMENT";
        static string mysql_pg_trx_problem_table = "PG_TRX_PROBLEM_1";
        static string oracle_pg_trx_payment_table = "pg_trx_payment@igskye";
        static string oracle_master_inquiry_table = "indovision.cust_inquiry@igateway";

        static string url_php_api_207 = "http://192.168.177.207/api_huda/action.php";

        static void Main(string[] args)
        {
            string a = get_max_trx_id();
            int max_id_mysql = Int32.Parse(a);
            int max_trx_id_oracle = Int32.Parse(get_max_idseq_trx_table_in_oracle());


            Console.WriteLine("ID_SEQ start_from = " + max_id_mysql + " <==> until ID_SEQ =  " + max_trx_id_oracle);
            if(max_id_mysql < max_trx_id_oracle)
            {
                read_data_trx_table(max_id_mysql, max_trx_id_oracle);
            }
            
            Console.WriteLine("Good Bye . . . . .");

            Thread.Sleep(2000);

        }

        static string get_max_trx_id()
        {
            string max_id = null;
            MySqlConnection conn = null;
            MySqlTransaction transaction = null;

            try
            {
                conn = new MySqlConnection(cs_mysql);
                conn.Open();
                transaction = conn.BeginTransaction();

                MySqlCommand command = new MySqlCommand();
                command.Connection = conn;
                command.Transaction = transaction;
                command.CommandText = @"SELECT MAX_ROW FROM "+mysql_max_row_trx_table+" WHERE ID='" + 2 + "'";


                MySqlDataReader reader = command.ExecuteReader();

                if (reader.Read())
                {
                    var result = reader.GetString(0);
                    max_id = result;
                }


            }
            catch (MySqlException ex)
            {
                try
                {
                    transaction.Rollback();

                }
                catch (MySqlException ex1)
                {
                    //Console.WriteLine("Error: {0}", ex1.ToString());
                    sb.Append("Error: {0}" + ex1.ToString() + Environment.NewLine);
                    
                    checking_file_log();

                    write_info_to_log_file();
                }

                Console.WriteLine("Error: {0}", ex.ToString());

            }
            finally
            {
                if (conn != null)
                {
                    conn.Close();
                }
            }

            return max_id;
        }
        static string get_icc_cust_nbr_from_prospect_table_mysql(string ten_digit_prospect_nbr)
        {
            string icc_cust_nbr = null;
            MySqlConnection conn = null;
            MySqlTransaction transaction = null;

            try
            {
                conn = new MySqlConnection(cs_mysql);
                conn.Open();
                transaction = conn.BeginTransaction();

                MySqlCommand command = new MySqlCommand();
                command.Connection = conn;
                command.Transaction = transaction;
                command.CommandText = @"SELECT REF_CUST_ID FROM PROSPECT WHERE PROSPECT_NBR='" + ten_digit_prospect_nbr + "'";


                MySqlDataReader reader = command.ExecuteReader();

                if (reader.Read())
                {
                    var result = reader.GetString(0);
                    icc_cust_nbr = result;
                }


            }
            catch (MySqlException ex)
            {
                try
                {
                    transaction.Rollback();

                }
                catch (MySqlException ex1)
                {
                    //Console.WriteLine("Error: {0}", ex1.ToString());
                    sb.Append("Error: {0}" + ex1.ToString() + Environment.NewLine);

                    //assign_info_to_variable("Error: {0}" + ex1.ToString() + Environment.NewLine);

                    checking_file_log();

                    write_info_to_log_file();
                }

                Console.WriteLine("Error: {0}", ex.ToString());

            }
            finally
            {
                if (conn != null)
                {
                    conn.Close();
                }
            }

            return icc_cust_nbr;
        }
        static void read_data_trx_table(int min_seq_id, int max_seq_id)
        {
           string[] data = new string[12];
            OracleConnection conn = null;
            OracleTransaction transaction = null;

            for (int i = min_seq_id + 1; i <= max_seq_id; i++)
            {
                Console.WriteLine(i);
                try
                {
                    conn = new OracleConnection(cs_oracle);
                    conn.Open();
                    transaction = conn.BeginTransaction();


                    OracleCommand cmd = new OracleCommand();
                    cmd.Connection = conn;
                    cmd.CommandText = "SELECT * FROM "+ oracle_pg_trx_payment_table + " WHERE ID_PAY = " + ":min_seq_id_param";
                    cmd.CommandType = CommandType.Text;
                    cmd.Parameters.Add("min_seq_id_param", i);
                    cmd.ExecuteNonQuery();
                    
                    OracleDataReader dr = cmd.ExecuteReader();

                    while (dr.Read())
                    {
                        int the_field_count = dr.FieldCount;
                        for (int a = 0; a < dr.FieldCount; a++)
                        {
                            //Console.WriteLine(dr.GetValue(a));
                            data[a] = dr.GetValue(a).ToString();
                        }


                        var cust_id = cust_no_convertion(data[3]);
                        
                        string merchant_bank = "INDOMARET";

                        try
                        {
                            string api_cust_detail = get_request_to_api("http://192.168.177.185:1111/api/iccapi/ap1msky!/customer/GetCustDetailByCustId/" + cust_id);
                            dynamic JsonResult = JsonConvert.DeserializeObject(api_cust_detail);
                            cust_id = JsonResult.Id;


                            #region do payment to icc
                            string the_json = get_request_to_api("http://192.168.177.185:1111/api/iccapi/ap1msky!/payment/doPaymentWithFulfillQuote/" + cust_id + "/" + data[4].ToString() + "/" + merchant_bank);
                            dynamic parsedJson = JsonConvert.DeserializeObject(the_json);
                            //Console.WriteLine(parsedJson); // show the json result from ICC API 
                            if (parsedJson == null)
                            {
                                Console.WriteLine("no answer from ICC API");
                                break;
                            }
                            else if (parsedJson != null)
                            {
                                Console.WriteLine("cust ID = " + data[3] + "| CUST CONVERTION = " + cust_id + " | Amount = " + data[4] + "| MERCHANT = " + merchant_bank);


                                #region copy data to mysql mirror table
                                do_insert_to_pg_trx_payment_mysql(data);
                                #endregion copy data to mysql mirror table
                                
                                sb.Append(cust_id + '#' + data[4] + '#' + data[11] + '#' + merchant_bank + "#" + DateTime.Now.ToString("dd_M_yyyy_HH:mm:ss") + Environment.NewLine);
                                
                                checking_file_log();

                                write_info_to_log_file();


                            }

                            #endregion do payment to icc
                        }
                        catch (Exception ex)
                        {
                            // Console.WriteLine("Error: {0}", ex.ToString());
                            #region insert data to mysql problem table
                            do_insert_to_pg_trx_problem_mysql(data);
                            #endregion insert data to mysql problem table

                            sb.Append("insert to problem table#" + cust_id + '#' + data[4] + '#' + data[11] + '#' + merchant_bank + "#" + DateTime.Now.ToString("dd_M_yyyy_HH:mm:ss") + Environment.NewLine);
                            
                            checking_file_log();

                            write_info_to_log_file();

                            
                        }

                        do_update_max_row_id_to_table(i.ToString());

                        #region update paid on master table
                        int c = count_cust_new_in_master_table_oracle(data[3]);

                        //if (c == 0)
                        //    insert_data_on_master_table_payment_oracle_using_api(data);
                        //else if (c > 0)
                        //    update_data_on_master_table_payment_oracle_using_api(data);

                        update_data_on_master_table_payment_oracle_using_api(data);

                        #endregion update paid on master table


                    }

                }
                catch (OracleException ex)
                {
                    try
                    {
                        transaction.Rollback();

                    }
                    catch (OracleException ex1)
                    {
                        Console.WriteLine("Error: {0}", ex1.ToString());
                        sb.Append("Error: {0}" + ex1.ToString() + "#catch (OracleException ex1)" + Environment.NewLine);

                        checking_file_log();

                        write_info_to_log_file();

                    }

                    Console.WriteLine("Error: {0}", ex.ToString());

                }
                finally
                {
                    if (conn != null)
                    {
                        conn.Close();
                    }
                }


                sb.Append(Environment.NewLine + Environment.NewLine);

                checking_file_log();

                write_info_to_log_file();
                //Console.WriteLine("paused 5 minutes");
                //Thread.Sleep(2000);
            }
            



        }
        static void do_insert_to_pg_trx_payment_mysql(string[] data)
        {
            MySqlConnection conn = null;
            MySqlTransaction transaction = null;

            try
            {
                conn = new MySqlConnection(cs_mysql);
                conn.Open();
                transaction = conn.BeginTransaction();


                MySqlCommand command = new MySqlCommand();
                command.Connection = conn;
                command.Transaction = transaction;

                command.CommandText = @"INSERT INTO "+ mysql_pg_trx_mirror_table +" (ID_PAY, PAY_RECEIVED_ID, PAYMENT_DATE, CUSTOMER_NBR, AMOUNT, PAID, PPOINT_ID, SERVICE_FROM, SERVICE_TO, POSTED, CREATED_BY,  CREATED_DATE) VALUES (?ID_PAY, ?PAY_RECEIVED_ID, ?PAYMENT_DATE, ?CUSTOMER_NBR, ?AMOUNT, ?PAID, ?PPOINT_ID, ?SERVICE_FROM, ?SERVICE_TO, ?POSTED, ?CREATED_BY, ?CREATED_DATE)";

                command.Parameters.AddWithValue("?ID_PAY", data[0]);
                command.Parameters.AddWithValue("?PAY_RECEIVED_ID", data[1]);
                command.Parameters.AddWithValue("?PAYMENT_DATE", data[2]);
                command.Parameters.AddWithValue("?CUSTOMER_NBR", data[3]);
                command.Parameters.AddWithValue("?AMOUNT", data[4]);
                command.Parameters.AddWithValue("?PAID", data[5]);
                command.Parameters.AddWithValue("?PPOINT_ID", data[6]);
                command.Parameters.AddWithValue("?SERVICE_FROM", data[7]);
                command.Parameters.AddWithValue("?SERVICE_TO", data[8]);
                command.Parameters.AddWithValue("?POSTED", data[9]);
                command.Parameters.AddWithValue("?CREATED_BY", 0);
                command.Parameters.AddWithValue("?CREATED_DATE", data[11]);


                command.ExecuteNonQuery();
                transaction.Commit();

                Console.WriteLine("successfully inserted to PG_TRX_PAYMENT");
                sb.Append("successfully inserted to PG_TRX_PAYMENT" + Environment.NewLine);

                checking_file_log();

                write_info_to_log_file();

            }
            catch (MySqlException ex)
            {
                try
                {
                    transaction.Rollback();

                }
                catch (MySqlException ex1)
                {
                    //Console.WriteLine("Error: {0}", ex1.ToString());
                    sb.Append("Error: {0}" + ex1.ToString() + Environment.NewLine);
                    
                    checking_file_log();

                    write_info_to_log_file();
                }

                Console.WriteLine("Error: {0}", ex.ToString());

            }
            finally
            {
                if (conn != null)
                {
                    conn.Close();
                }
            }

        }
        static string get_request_to_api(string url)
        {
            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);
            try
            {
                WebResponse response = request.GetResponse();
                using (Stream responseStream = response.GetResponseStream())
                {
                    StreamReader reader = new StreamReader(responseStream, Encoding.UTF8);
                    return reader.ReadToEnd();
                }
            }
            catch (WebException ex)
            {
                WebResponse errorResponse = ex.Response;
                using (Stream responseStream = errorResponse.GetResponseStream())
                {
                    StreamReader reader = new StreamReader(responseStream, Encoding.GetEncoding("utf-8"));
                    String errorText = reader.ReadToEnd();
                    // log errorText
                    sb.Append(errorText + Environment.NewLine);
                    
                    checking_file_log();

                    write_info_to_log_file();
                }
                throw;
            }
        }
        static int count_cust_new_in_master_table_oracle(string cust_id_param)
        {
            OracleConnection conn = null;
            OracleTransaction transaction = null;

            //int count = 0;
            int data_exist = 1;
            try
            {
                conn = new OracleConnection(cs_oracle);
                conn.Open();

                var commandText = "Select count(*) from "+oracle_master_inquiry_table+" where CUST_NEW = :SomeValue";

                using (OracleConnection connection = new OracleConnection(cs_oracle))
                using (OracleCommand command = new OracleCommand(commandText, connection))
                {
                    command.CommandType = CommandType.Text;
                    command.Parameters.Add("SomeValue", cust_id_param);
                    command.Connection.Open();
                    
                    //command.ExecuteNonQuery();
                    object count = command.ExecuteScalar();


                    if (count.ToString() == "0") //kondisi jika blm ada data
                        data_exist = 0;

                    command.Connection.Close();

                }

            }
            catch (OracleException ex)
            {
                try
                {
                    transaction.Rollback();


                }
                catch (OracleException ex1)
                {
                    Console.WriteLine("Error: {0}", ex1.ToString());
                }

                Console.WriteLine("Error: {0}", ex.ToString());

            }
            finally
            {
                if (conn != null)
                {
                    conn.Close();
                }
            }


            return data_exist;
        }
        static int count_cust_old_in_master_table_oracle(string cust_id_param)
        {
            OracleConnection conn = null;
            OracleTransaction transaction = null;

            //int count = 0;
            int data_exist = 1;
            try
            {
                conn = new OracleConnection(cs_oracle);
                conn.Open();

                var commandText = "Select count(*) from " + oracle_master_inquiry_table + " where CUST_OLD = :SomeValue";

                using (OracleConnection connection = new OracleConnection(cs_oracle))
                using (OracleCommand command = new OracleCommand(commandText, connection))
                {
                    command.CommandType = CommandType.Text;
                    command.Parameters.Add("SomeValue", cust_id_param);
                    command.Connection.Open();

                    //command.ExecuteNonQuery();
                    object count = command.ExecuteScalar();


                    if (count.ToString() == "0") //kondisi jika blm ada data
                        data_exist = 0;

                    command.Connection.Close();

                }

            }
            catch (OracleException ex)
            {
                try
                {
                    transaction.Rollback();


                }
                catch (OracleException ex1)
                {
                    Console.WriteLine("Error: {0}", ex1.ToString());
                }

                Console.WriteLine("Error: {0}", ex.ToString());

            }
            finally
            {
                if (conn != null)
                {
                    conn.Close();
                }
            }


            return data_exist;
        }
        static void do_update_max_row_id_to_table(string max_quote_id_param)
        {
            MySqlConnection conn = null;
            MySqlTransaction transaction = null;

            try
            {
                conn = new MySqlConnection(cs_mysql);
                conn.Open();
                transaction = conn.BeginTransaction();

                MySqlCommand command = new MySqlCommand();
                command.Connection = conn;
                command.Transaction = transaction;
                command.CommandText = @"UPDATE " + mysql_max_row_trx_table + " SET MAX_ROW='" + max_quote_id_param + "'  WHERE ID='" + 2 + "'";

                command.ExecuteNonQuery();
                transaction.Commit();

                Console.WriteLine("successfully updated to MAX_ROW_TRX");
                sb.Append("successfully updated to MAX_ROW_TRX" + Environment.NewLine);

                checking_file_log();

                write_info_to_log_file();

            }
            catch (MySqlException ex)
            {
                try
                {
                    transaction.Rollback();

                }
                catch (MySqlException ex1)
                {
                    sb.Append("Error: {0}" + ex1.ToString() + Environment.NewLine);
                    
                    checking_file_log();

                    write_info_to_log_file();
                }

                Console.WriteLine("Error: {0}", ex.ToString());

            }
            finally
            {
                if (conn != null)
                {
                    conn.Close();
                }
            }

        }
        static string get_max_idseq_trx_table_in_oracle()
        {
            string max_id_seq = null;
            OracleConnection conn = null;
            OracleTransaction transaction = null;

            try
            {
                conn = new OracleConnection(cs_oracle);
                conn.Open();
                transaction = conn.BeginTransaction();


                OracleCommand cmd = new OracleCommand();
                cmd.Connection = conn;
                cmd.CommandText = "SELECT MAX(ID_PAY) FROM "+ oracle_pg_trx_payment_table + "";
                cmd.CommandType = CommandType.Text;
                cmd.ExecuteNonQuery();


                OracleDataReader dr = cmd.ExecuteReader();



                while (dr.Read())
                {
                    int the_field_count = dr.FieldCount;
                    for (int a = 0; a < dr.FieldCount; a++)
                    {
                        //Console.WriteLine(dr.GetValue(a));
                        max_id_seq = dr.GetValue(0).ToString();
                    }


                }

                //Console.Read();



            }
            catch (OracleException ex)
            {
                try
                {
                    transaction.Rollback();

                }
                catch (OracleException ex1)
                {
                    //Console.WriteLine("Error: {0}", ex1.ToString());
                    sb.Append("Error: {0}" + ex1.ToString() + Environment.NewLine);
                    
                    checking_file_log();

                    write_info_to_log_file();
                }

                Console.WriteLine("Error: {0}", ex.ToString());

            }
            finally
            {
                if (conn != null)
                {
                    conn.Close();

                }
            }
            return max_id_seq;
        }
        static void update_data_on_master_table_payment_oracle_using_api(string[] data_trx_param)
        {
            var cust_new = cust_no_convertion(data_trx_param[3]);

            string the_json = get_request_to_api("http://192.168.177.185:1111/api/iccapi/ap1msky!/customer/GetCustDetailByCustId/" + cust_new);
            dynamic parsedJson = JsonConvert.DeserializeObject(the_json);

            var icc_cust_nbr = parsedJson.Id;
            var icc_cust_full_name = parsedJson.DefaultAddress.FirstName + parsedJson.DefaultAddress.MiddleName + parsedJson.DefaultAddress.Surname;

            using (var client = new WebClient())
            {
                var values = new NameValueCollection();
                values["action"] = "3";
                values["cust_id"] = data_trx_param[3];
                //values["cust_name"] = "dari table trx indomaret";
                //values["amount"] = data_trx_param[4];
                //values["sfrom"] = data_trx_param[7];
                //values["sto"] = data_trx_param[8];
                values["paid"] = "1";

                var response = client.UploadValues(url_php_api_207, values);

                var responseString = Encoding.Default.GetString(response);

                sb.Append(responseString + Environment.NewLine);
               
                checking_file_log();

                write_info_to_log_file();

                //dynamic parsedJson = JsonConvert.DeserializeObject(responseString);
            }
        }
        static void insert_data_on_master_table_payment_oracle_using_api(string[] data_trx_param)
        {
            var cust_new = cust_no_convertion(data_trx_param[3]);

            string the_json = get_request_to_api("http://192.168.177.185:1111/api/iccapi/ap1msky!/customer/GetCustDetailByCustId/"+ cust_new);
            dynamic parsedJson = JsonConvert.DeserializeObject(the_json);

            var icc_cust_nbr = parsedJson.Id;
            var icc_cust_full_name = parsedJson.DefaultAddress.FirstName + parsedJson.DefaultAddress.MiddleName + parsedJson.DefaultAddress.Surname;
            
            using (var client = new WebClient())
            {
                var values = new NameValueCollection();
                values["action"] = "1";
                values["cust_old"] = data_trx_param[3];
                values["cust_new"] = icc_cust_nbr;
                values["firstname"] = parsedJson.DefaultAddress.FirstName;
                values["middlename"] = parsedJson.DefaultAddress.MiddleName;
                values["surname"] = parsedJson.DefaultAddress.Surname;
                values["amount"] = data_trx_param[4];
                values["sfrom"] = data_trx_param[7];
                values["sto"] = data_trx_param[8];
                values["paid"] = "1";

                var response = client.UploadValues(url_php_api_207, values);

                var responseString = Encoding.Default.GetString(response);

                //createText_temp = createText;
                //createText = createText_temp + insert_or_update_assign_to_string(customer_id, customer_name, total_amount, start_date.ToString(), end_date.ToString(), "99") + "###" + responseString + "###" + DateTime.Now.ToString("dd_M_yyyy_HH:mm:ss") + Environment.NewLine;
                sb.Append(responseString + Environment.NewLine);
                
                checking_file_log();

                write_info_to_log_file();

                //dynamic parsedJson = JsonConvert.DeserializeObject(responseString);
            }
        }
        static void assign_info_to_variable(string information)
        {
            createText_temp = createText;
            createText = createText_temp + information;
        }
        static void write_info_to_log_file()
        {
            File.AppendAllText(full_path_file, sb.ToString());
            sb.Clear();

            //File.WriteAllText(full_path_file, createText);
        }
        static void checking_file_log()
        {
            #region checking file log
            if (!Directory.Exists(path))  // if it doesn't exist, create
                Directory.CreateDirectory(path);

            file_log_name = DateTime.Now.ToString("dd_M_yyyy").ToString();

            full_path_file = path + "PG_" + file_log_name + ".txt";

            if (!File.Exists(full_path_file))
            {
                File.Create(full_path_file).Dispose();
                using (TextWriter tw = new StreamWriter(full_path_file))
                {
                    tw.WriteLine("The very first line!" + Environment.NewLine);
                    tw.Close();
                }
            }
            //else if (File.Exists(full_path_file))
            //{
            //    using (TextWriter tw = new StreamWriter(full_path_file))
            //    {
            //        tw.WriteLine("The next line!");
            //        tw.Close();
            //    }
            //}
            #endregion checking file log
        }
        static string get_new_cust_nbr(string old_cust_nbr)
        {
            string amount = null;
            OracleConnection conn = null;
            OracleTransaction transaction = null;

            try
            {
                conn = new OracleConnection(cs_oracle);
                conn.Open();
                transaction = conn.BeginTransaction();


                OracleCommand cmd = new OracleCommand();
                cmd.Connection = conn;
                cmd.CommandText = "SELECT * FROM "+ oracle_master_inquiry_table+" WHERE CUST_OLD = " + ":icc_cust_nbr";
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Add("icc_cust_nbr", old_cust_nbr);
                cmd.ExecuteNonQuery();

                int c = count_cust_new_in_master_table_oracle(old_cust_nbr);

                Console.WriteLine("total row = " + c);

                OracleDataReader dr = cmd.ExecuteReader();

                while (dr.Read())
                {
                    int the_field_count = dr.FieldCount;
                    for (int a = 0; a < dr.FieldCount; a++)
                    {
                        Console.WriteLine(dr.GetValue(a));
                        amount = dr.GetValue(1).ToString(); //get value from cust_new_field
                    }


                }

                //Console.Read();



            }
            catch (OracleException ex)
            {
                try
                {
                    transaction.Rollback();

                }
                catch (OracleException ex1)
                {
                    Console.WriteLine("Error: {0}", ex1.ToString());
                }

                Console.WriteLine("Error: {0}", ex.ToString());

            }
            finally
            {
                if (conn != null)
                {
                    conn.Close();
                }
            }
            return amount;
        }
        static string cust_no_convertion(string cust_nbr)
        {
            string result = null;
            
            if (cust_nbr.Length != 9)
            {
                //data[3] = get_new_cust_nbr(data[3]);
                
                if (cust_nbr.Length == 12)
                {
                    String a = cust_nbr.Substring(0, 4);
                    String x = null;

                    switch (a)
                    {
                        case "4010":
                            x = "3";
                            break;
                        case "3010":
                            x = "1";
                            break;
                        case "3019":
                            x = "1";
                            break;
                        case "3020":
                            x = "2";
                            break;
                        case "3040":
                            x = "4";
                            break;
                    }

                    //if (a == "4010")
                    //{
                    //    x = "3";
                    //}
                    //else if (a == "3010")

                    //{
                    //    x = "1";
                    //}
                    //else if (a == "3020")
                    //{
                    //    x = "2";
                    //}
                    //else if (a == "3040")
                    //{
                    //    x = "4";
                    //}
                    String y = cust_nbr.Substring(4, 8);
                    result = x + y;
                }

                if (cust_nbr.Length == 10) // for handle cust_no of prospect
                {
                    result = get_icc_cust_nbr_from_prospect_table_mysql(cust_nbr);
                }
            }
            if (cust_nbr.Length == 9)
                result = cust_nbr;
            //else
            //    result = "not_covered";

            return result;
        }
        static void do_insert_to_pg_trx_problem_mysql(string[] data)
        {
            MySqlConnection conn = null;
            MySqlTransaction transaction = null;

            try
            {
                conn = new MySqlConnection(cs_mysql);
                conn.Open();
                transaction = conn.BeginTransaction();

                MySqlCommand command = new MySqlCommand();
                command.Connection = conn;
                command.Transaction = transaction;

                command.CommandText = @"INSERT INTO "+ mysql_pg_trx_problem_table +" (ID_PAY, PAY_RECEIVED_ID, PAYMENT_DATE, CUSTOMER_NBR, AMOUNT, PAID, PPOINT_ID, SERVICE_FROM, SERVICE_TO, POSTED, CREATED_BY,  CREATED_DATE) VALUES (?ID_PAY, ?PAY_RECEIVED_ID, ?PAYMENT_DATE, ?CUSTOMER_NBR, ?AMOUNT, ?PAID, ?PPOINT_ID, ?SERVICE_FROM, ?SERVICE_TO, ?POSTED, ?CREATED_BY, ?CREATED_DATE)";

                command.Parameters.AddWithValue("?ID_PAY", data[0]);
                command.Parameters.AddWithValue("?PAY_RECEIVED_ID", data[1]);
                command.Parameters.AddWithValue("?PAYMENT_DATE", data[2]);
                command.Parameters.AddWithValue("?CUSTOMER_NBR", data[3]);
                command.Parameters.AddWithValue("?AMOUNT", data[4]);
                command.Parameters.AddWithValue("?PAID", data[5]);
                command.Parameters.AddWithValue("?PPOINT_ID", data[6]);
                command.Parameters.AddWithValue("?SERVICE_FROM", data[7]);
                command.Parameters.AddWithValue("?SERVICE_TO", data[8]);
                command.Parameters.AddWithValue("?POSTED", data[9]);
                command.Parameters.AddWithValue("?CREATED_BY", 0);
                command.Parameters.AddWithValue("?CREATED_DATE", data[11]);


                command.ExecuteNonQuery();
                transaction.Commit();

                Console.WriteLine("successfully inserted to PG_TRX_PROBLEM");
                sb.Append("successfully inserted to PG_TRX_PROBLEM" + Environment.NewLine);

                checking_file_log();

                write_info_to_log_file();

            }
            catch (MySqlException ex)
            {
                try
                {
                    transaction.Rollback();

                }
                catch (MySqlException ex1)
                {
                    //Console.WriteLine("Error: {0}", ex1.ToString());
                    sb.Append("Error: {0}" + ex1.ToString() + Environment.NewLine);
                    
                    checking_file_log();

                    write_info_to_log_file();
                }

                Console.WriteLine("Error: {0}", ex.ToString());

            }
            finally
            {
                if (conn != null)
                {
                    conn.Close();
                }
            }

        }
    }
}
