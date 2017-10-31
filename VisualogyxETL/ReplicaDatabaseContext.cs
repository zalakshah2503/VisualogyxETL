using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Text;

namespace VisualogyxETL
{
    class ReplicaDatabaseContext : DbContext
    {
        public ReplicaDatabaseContext()
        {

        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseSqlServer(@"Server=(localdb)\mssqllocaldb;Database=ReplicaDatabase;Trusted_Connection=True;");
        }
    }
}
