using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PADIMapNoReduceLibs
{
    public interface IWorker
    {
        void SubmitJobToWorker(long start, long end, int split);
        bool SendMapper(String className, byte[] code);
        void SubmitJob(long fileSize, int splits, String className, byte[] code);
    }
}
