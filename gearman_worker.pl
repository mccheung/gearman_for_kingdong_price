#!/usr/local/bin/perl
use strict;
use warnings;
use Data::Dumper;

use Coro;
use Coro::Timer;
use Coro::LWP;
use LWP::UserAgent;
use LWP::ConnCache;
use Image::OCR::Tesseract 'get_ocr';
use KingDong;
use Gearman::Worker;
use JSON;

my $MAX_THREADS = 30;
my @data_queue;
my @result_queue;
my $processing_count = 0;

init_coro($MAX_THREADS);

my $worker = Gearman::Worker->new();
$worker->job_servers('66.175.220.222');
$worker->register_function( fetch_price => \&fetch_price);

$worker->work while 1;


sub fetch_price {
    my $jobs = shift;
    my $ids = $jobs->arg;
    my $json = decode_json($ids);

    my @results;

    last unless scalar @$json;
    foreach (@$json){
       if ($#data_queue > $MAX_THREADS * 2) {
            Coro::Timer::sleep(0.02);
            redo;
        }

        push(@data_queue, $_);
        if ($#result_queue > -1) {
            warn $#result_queue;
            while ($#result_queue > -1) {
                my $res = shift(@result_queue);
                print "Sku: $res->{sku_id}\tPrice: $res->{price}\n";
                push @results, $res;
           }
        }
    }

    while ($processing_count > 0 or $#data_queue > -1 or $#result_queue > -1) {
        while ($#result_queue > -1) {
            my $res = shift(@result_queue);
            print "Sku: $res->{sku_id}\tPrice: $res->{price}\n";
            push @results, $res;
        }
        Coro::Timer::sleep(0.02);
    }

    return encode_json(\@results);

}



sub init_coro {
    my ($max) = shift;
    foreach (1 .. $max) {
        async(\&thread_io);
    }
}


sub thread_io()
{
    my $lwp = LWP::UserAgent->new();
    my $conncache = new LWP::ConnCache;
    $lwp->conn_cache($conncache);

    while (1) {
        if ($#data_queue == -1) {
            Coro::Timer::sleep(1);
            next;
        }

        my $sku_id = shift(@data_queue);
        ++$processing_count;

        my $url = price_url($sku_id);
        my $path = get_path($sku_id);
        my $file = $path .  "/$sku_id" . '.png';
        my $resp = $lwp->get($url);
        open (my $fh, '>', $file) || die "$!\n";
        binmode($fh);
        print $fh $resp->content;
        close($fh);

        my $price;
        eval {
            $price = get_ocr($file);
            $price = $1 if $price =~ /(\d+\.?\d*)/;
            $price = $1 if $price > 5000 && $price =~ /^51(\d+\.\d*)/;
        };

        #print "Ocr: $file\t$price\n";
        push(@result_queue, {sku_id => $sku_id, price => $price});

        unlink ($file);
        --$processing_count;
    }
}


