use ExtUtils::MakeMaker;
WriteMakefile(
   NAME => 'Metadata::DB::Search',
   VERSION_FROM  => 'lib/Metadata/DB/Search.pm',
   PREREQ_PM => {
      'Metadata::DB' => '1.10',
      'Test::Simple' => '0.7',
      'LEOCHARRE::Class' => '0',
      'LEOCHARRE::DEBUG' => '1.11',
      'Cwd' => '3',
      'DBI' => '1',
      'Smart::Comments' => '1',
   },
);
