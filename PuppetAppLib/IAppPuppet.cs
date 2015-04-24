using MapLib;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PuppetAppLib
{
    public interface IAppPuppet
    {
        void Submit(String entry_url, String inputFile, String outputDirectory, Int32 splits, String mapClassName, IMap mapObject);
    }                                                                                                                                                                                                                                                                             
}
