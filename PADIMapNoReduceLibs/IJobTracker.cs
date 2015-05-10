using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PADIMapNoReduceLibs
{
    public interface IJobTracker
    {
        IList<long> GetSplitRange();
        void SubmitJob(long fileSize, int splits, String className, byte[] code, String clientURL);
        void RegisterWorker(int id, String url);
        void StatusRequest();
        void Freeze();
        void Unfreeze();
        void ReceiveImAlive(int id);
    }
}
