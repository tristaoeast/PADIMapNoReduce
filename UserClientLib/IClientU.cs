using MapLib;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UserClientLib
{
    public interface IClientU
    {
        void Submit(String inputFile, int splits, String outputDirectory, IMap mapObject);
        void Init(String entryUrl);
    }
}
