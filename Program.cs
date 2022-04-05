using System;
using System.Collections.Generic;

namespace import_CSV_bleh
{
    class Program
    {
        static void Main(string[] args)
        {
            string tablename = null;
            string filename = null;
            bool skip_header = false;
            string databasename = "example.db";
            List<string> column_headings = new List<string>();
            List<string> dup_keys = new List<string>();
            string delim = ",";
            bool emptyfail = false;
            bool clear_related = false;

            Console.WriteLine("Starting up");
            int i = 0;
            while (i < args.Length)
            {
                if (match("--file", args[i]))
                {
                    filename = args[i + 1];
                    i = i + 2;
                }
                else if (match("--table", args[i]))
                {
                    tablename = args[i + 1];
                    i = i + 2;
                }
                else if (match("--database", args[i]))
                {
                    databasename = args[i + 1];
                    i = i + 2;
                }
                else if (match("--dup-key", args[i]))
                {
                    dup_keys.Add(args[i + 1]);
                    i = i + 2;
                }
                else if (match("--separater", args[i]))
                {
                    delim = args[i + 1];
                    i = i + 2;
                }
                else if (match("--skip_header", args[i]))
                {
                    skip_header = true;
                    i = i + 1;
                }
                else if (match("--empty-fail", args[i]))
                {
                    emptyfail = true;
                    i = i + 1;
                }
                else if (match("--clear-related", args[i]))
                {
                    clear_related = true;
                    i = i + 1;
                }
                else if (match("--verbose", args[i]))
                {
                    int index = 0;
                    foreach(string s in args)
                    {
                        Console.WriteLine(string.Format("argument {0}: {1}", index, s));
                        index = index + 1;
                    }
                    i = i + 1;
                }
                else if (match("--columns", args[i]))
                {
                    i = i + 1;
                    while (i < args.Length)
                    {
                        column_headings.Add(args[i]);
                        i = i + 1;
                    }
                }
                else
                {
                    i = i + 1;
                }
            }

            Console.WriteLine(string.Format("Starting import of file: \"{0}\"\nUsing Database: \"{1}\"", filename, databasename));
            CSV_Import.init(filename, tablename, databasename, skip_header, column_headings, delim, emptyfail, dup_keys, clear_related);
            bool l = true;
            while (l)
            {
                l = CSV_Import.read_record(emptyfail);
            }

            CSV_Import.cleanup();
            Console.WriteLine(string.Format("import of {0} into {1} successful", filename, tablename));
        }

        static public bool match(string a, string b)
        {
            return a.Equals(b, StringComparison.CurrentCultureIgnoreCase);
        }
    }
}
