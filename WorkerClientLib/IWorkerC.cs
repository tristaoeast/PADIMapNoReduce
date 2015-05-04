using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WorkerClientLib
{
    public interface IWorkerC
    {
        void SubmitJob(long fileSize, int splits, String className, byte[] code);
    }
}
