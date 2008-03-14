use Test::Simple 'no_plan';
require './t/testlib.pl';
use strict;
use lib './lib';
#use Smart::Comments '###';
use Metadata::DB::Search;


$Metadata::DB::Search::DEBUG = 1;

my $dbh = _get_new_handle();
ok($dbh,'got dbh') or die;
my $s = Metadata::DB::Search->new({ DBH => $dbh });
ok($s,'instanced');


my $search_num = 0;

test_search( {
   'age:exact'  => 18,
   'eyes' => 'blue',
   'name' => 'a',
});


test_search( {
   'age:exact'  => 18,
   'eyes:exact' => 'hazel',
   'cup:exact'  => 'A',
});

test_search( {
   'age:morethan'  => 25,
   'eyes:exact' => 'blue',
});


test_search( {
   'age:morethan'  => 20,
   'age:lessthan'  => 24,

});


exit;




# ONE MORE; SEARCH MULTIPLE POSSIBILITIES...
#

  my $s = Metadata::DB::Search->new({ DBH => $dbh });
   ok($s,'instanced');

$s->search({
   hair => ['blonde','redhead'],
});

for my $id (@{$s->ids}){
   my $m = $s->_record_entries_hashref($id);
   print STDERR " name $$m{name}, age $$m{age}, hair $$m{hair}\n"; 
}



exit;






sub test_search {
   my $s_meta = shift; # arg is search params

   printf STDERR "\n\n========== SEARCH NUMBER %s\n\n", ++$search_num;
   ### $s_meta

   my $s = Metadata::DB::Search->new({ DBH => $dbh });
   ok($s,'instanced');

   my $val = $s->search( $s_meta );
   ok($val, "search() returns a value [$val]");

   my $hits = $s->ids_count;
   ok($hits, "got $hits");

   my $got =0;
	for my $id (@{$s->ids}){
	   my $r_meta = $s->_record_entries_hashref($id);
	   ref $r_meta eq 'HASH' or die("something wrong, does id exist? $id - or problem in Metadata::DB::Base");
	
	   while ( my($s_att, $s_val) = each %$s_meta ) {
         my $search_type = 'like';
	      $s_att=~s/\:(.+)$// and $search_type = $1;
                  
	      defined $r_meta->{$s_att} or die("att $s_att is not present in result item");

	      my @r_val = @{ $r_meta->{$s_att} };
	      defined @r_val or die("att $s_att is present in result item as [@r_val]");
         #print STDERR " [ vals @r_val]\n";


         if ($search_type eq 'lessthan'){
   	      my $found_matching_val =0;
	         for( @r_val ){            
	            if ( $_ <  $s_val ){
	               $found_matching_val = 1;
	               last;
	            }
	         }	
   	      $found_matching_val or die("we sougth $s_att [$s_val], we got vals [@r_val], search type $search_type");   
         }


         elsif ($search_type eq 'morethan'){
   	      my $found_matching_val =0;
	         for( @r_val ){            
	            if ( $_ >  $s_val ){
	               $found_matching_val = 1;
	               last;
	            }
	         }	
   	      $found_matching_val or die("we sougth $s_att [$s_val], we got vals [@r_val], search type $search_type");   
         }

	
         elsif ($search_type eq 'exact'){
   	      my $found_matching_val =0;
	         for( @r_val ){            
	            if ( "$_" eq $s_val ){
	               $found_matching_val = 1;
	               last;
	            }
	         }	
   	      $found_matching_val or die("we sougth $s_att [$s_val], we got vals [@r_val], search type $search_type");   
         }

         elsif ($search_type eq 'like'){
   	      my $found_matching_val =0;
	         for( @r_val ){            
	            if ( $_=~/$s_val/i ){
	               $found_matching_val = 1;
	               last;
	            }
	         }	
   	      $found_matching_val or die("we sougth $s_att [$s_val], we got vals [@r_val], search type $search_type");   
         }


	   }
	   $got++;
	}
	
	my $idco = $s->ids_count;
	ok($idco, "ids_count() returns");
	$got == $s->ids_count or die("got[$got] and id count [$idco] don't match");
	
	my $cks;
	ok( $cks = $s->constriction_keys," got constriction keys: @$cks") or die;
   
   return 1;
}









