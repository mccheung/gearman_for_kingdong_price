#!/usr/local/bin/perl
use strict;
use warnings;
use Data::Dumper;

use JSON;
use KingDong;
use Gearman::Client;
use List::Util qw/max/;

my $MAX_THREADS = 30;
my @data_queue;
my @result_queue;
my $processing_count = 0;
my $client = Gearman::Client->new();
$client->job_servers('66.175.220.222');

my $tasks = $client->new_task_set;

my $last_id = 0;
my $items = 1;
while (1){
    my $ids = get_skus(1000, $last_id);
    last unless scalar @$ids;
    $last_id = max(@$ids);

    my $json = encode_json($ids);

    $tasks->add_task(
        fetch_price => $json,
        {
            on_complete => \&do_complete
        },
    );
    print "Added: $items \n";
    $items++;

}
$tasks->wait;


sub do_complete {
    my $result = shift;
    my $json = from_json($$result);

    foreach (@$json){
        my $sku_id = $_->{sku_id};
        my $price = $_->{price} || 0;
        print "SKU: $sku_id\tPrice: $price\n";

        if ($sku_id && $price > 0 ){
            update_price($sku_id, $price);
        }else{
            update_no_price($sku_id)
        }
    }
}
