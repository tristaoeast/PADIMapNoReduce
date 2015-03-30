using MapLib;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClientLib
{
    public interface IClient
    {
        IList<KeyValuePair<String, String>> Submit(String inputFilePath, int splits, String outputFilesDirectory, IMap mapClass);
    }
}
