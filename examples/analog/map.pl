#!/usr/bin/env perl

package Analog::Mapper;
use Moose;
with 'Hadoop::Mapper';

sub map {
    my ($self, $key, $value) = @_;

    my @segments = split /\s+/, $value;
    $self->emit($segments[8] => 1);
}

package main;
use FindBin::libs;

Analog::Mapper->run;
