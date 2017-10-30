using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Text;

namespace VisualogyxETL
{
    class OriginDatabaseContext : DbContext
    {
        public OriginDatabaseContext(DbContextOptions<OriginDatabaseContext> options)
            : base(options)
        {
        }

        public OriginDatabaseContext()
        {

        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseSqlServer(@"Server=(localdb)\mssqllocaldb;Database=OriginDatabase;Trusted_Connection=True;");
        }
    }
}
