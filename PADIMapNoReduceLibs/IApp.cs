using MapLib;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PADIMapNoReduceLibs
{
    public interface IApp
    {
        void Submit(String entry_url, String inputFile, String outputDirectory, int splits, String mapClassName, string dllPath);
        void notifyJobFinished(bool finished);
    }                                                                                                                                                                                                                                                                             
}
