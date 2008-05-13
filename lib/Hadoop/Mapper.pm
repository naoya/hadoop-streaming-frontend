package Hadoop::Mapper;
use Moose::Role;

use IO::Handle;
use Params::Validate qw/validate_pos/;

with 'Hadoop::Role::Emitter';
requires qw/map/;

sub run {
    my $class = shift;
    my $self = $class->new;

    ## FIXME: 入力の形式に併せて処理を変更
    while (my $line = STDIN->getline) {
        chomp $line;

        ## SequenceFileAsTextInputFormat
        #my ($key, $value) = split /\t/, $line;

        $self->map(undef, $line);
    }
}

sub emit {
    my ($self, $key, $value) = @_;
    eval {
        $self->put($key, $value);
    };
    if ($@) {
        warn $@;
    }
}

sub put {
    my ($self, $key, $value) = validate_pos(@_, 1, 1, 1);
    printf "%s\t%s\n", $key, $value;
}

1;
