﻿﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PADIMapNoReduceLibs
{
    public interface IWorker
    {
        int SubmitJobToWorker(long start, long end, int split, String clientURL);
        bool SendMapper(String className, byte[] code);
        void SubmitJobToTracker(long fileSize, int splits, String className, byte[] code, String clientURL);
        void RegisterWorker(int id, string url);
        void StatusRequest();
        void Freeze(bool jt);
        void Unfreeze(bool jt);
        String GetJobTrackerURL();
        void Slow(int secs);
    }
}
