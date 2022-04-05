using CsvHelper;
using CsvHelper.Configuration;
using sqlite_Example;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace import_CSV_bleh
{
    class CSV_Import
    {
        class dup_crap
        {
            public int key_index;
            public List<int> col_index;
            public string key_text;
            public string target_table;
            public string column_name;
            public string column_header;
        }

        static private List<dup_crap> issues_to_deal_with;
        static private SQLiteDatabase mydatabase;
        static private List<string> table_cols;
        static private int row_length;
        static private CsvReader csv;
        static private StreamReader reader;
        static private CsvConfiguration config;
        static private string table_name;
        static private bool End_Of_File;

        static public void init(string filename, string tablename, string databasename, bool skip_header, List<string> column_headers, string delim, bool emptyfail, List<string> dup_kys)
        {
            issues_to_deal_with = new List<dup_crap>();
            foreach(string line in dup_kys)
            {
                string[] set = line.Split(',');
                dup_crap d = new dup_crap();
                d.target_table = set[0];
                d.key_text = set[1];
                d.column_header = set[2];
                d.column_name = set[3];
                d.col_index = new List<int>();
                issues_to_deal_with.Add(d);
            }


            End_Of_File = false;
            if (tablename.Contains(' ') || tablename.Contains('\t') || tablename.Contains('\n') || tablename.Contains('\r'))
            {
                Console.WriteLine(string.Format("I refuse to deal with sql tables with white space in their names, even \"{0}\"\nSo fix that garbage", tablename));
                Environment.Exit(4);

            }
            else table_name = tablename;

            mydatabase = new SQLiteDatabase(databasename);
            // Otherwise lets get it
            config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                PrepareHeaderForMatch = args => args.Header.ToLower(),
                Delimiter = delim
            };
            try
            {
                reader = new StreamReader(filename);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Environment.Exit(3);
            }
            csv = new CsvReader(reader, config);

            // Do we need to read a header?
            if (skip_header) table_cols = column_headers;
            else table_cols = read_row();

            // Regardless use the column titles expressed
            if (column_headers.Count > 0) table_cols = column_headers;

            if ((null == table_cols) || (0 >= table_cols.Count))
            {
                Console.WriteLine(string.Format("I can't use an empty file named {0}", filename));
                if (emptyfail) Environment.Exit(5);
                else Environment.Exit(0);
            }

            // Figure out what columns to scrub
            foreach (dup_crap d in issues_to_deal_with)
            {
                find_the_issues(d);
            }

            foreach (dup_crap d in issues_to_deal_with)
            {
                remove_the_issues(d);
            }

            row_length = table_cols.Count();

            mydatabase.ExecuteNonQuery(string.Format("drop table if exists '{0}';", tablename));
            string sql = string.Format("CREATE TABLE '{0}' ( ", tablename);
            foreach (string s in table_cols)
            {
                if ('`' == sql[sql.Length - 1]) sql = sql + ", ";
                sql = sql + "`" + s + "`";
            }
            sql = sql + ");";

            mydatabase.ExecuteNonQuery(sql);

            foreach (dup_crap d in issues_to_deal_with)
            {
                mydatabase.ExecuteNonQuery(string.Format("drop table if exists '{0}'; ", d.target_table));
                mydatabase.ExecuteNonQuery(string.Format("CREATE TABLE '{0}' ( '{1}', '{2}' );", d.target_table, d.key_text, d.column_name));
            }


        }

        private static void remove_the_issues(dup_crap d)
        {
            table_cols.RemoveAll(item => match(item, d.column_header));
        }

        static private void find_the_issues(dup_crap d)
        {
            int i = 0;
            foreach(string s in table_cols)
            {
                if(match(s, d.key_text))
                {
                    d.key_index = i;
                }
                else if(match(s, d.column_header))
                {
                    d.col_index.Add(i);
                }

                i = i + 1;
            }
        }

        static public bool read_record()
        {
            List<string> entries = read_row();
            if (!End_Of_File) return End_Of_File;

            Dictionary<string, string> tmp = new Dictionary<string, string>();
            int i = 0;
            while (i < row_length)
            {
                tmp.Add(table_cols[i], entries[i]);
                i = i + 1;
            }

            mydatabase.Insert(table_name, tmp);

            foreach( dup_crap d in issues_to_deal_with)
            {
                foreach(int c in d.col_index)
                {
                    if (!match("", entries[c]))
                    {
                        Dictionary<string, string> tmp2 = new Dictionary<string, string>() { { d.key_text, entries[d.key_index] },
                                                                                        { d.column_name, entries[c] } };
                        mydatabase.Insert(d.target_table, tmp2);
                    }
                }
            }

            return End_Of_File;
        }

        static public void cleanup()
        {
            mydatabase.SQLiteDatabase_Close();
        }

        static private List<string> read_row()
        {
            End_Of_File = csv.Read();
            if (!End_Of_File) return null;

            List<string> s = new List<string>();
            int i = 0;
            int r = csv.Parser.Count;
            while (i < r)
            {
                string t = csv.GetField(i);
                s.Add(t);
                i = i + 1;
            }
            return s;

        }

        static private bool match(string a, string b)
        {
            return a.Equals(b, StringComparison.CurrentCultureIgnoreCase);
        }
    }
}