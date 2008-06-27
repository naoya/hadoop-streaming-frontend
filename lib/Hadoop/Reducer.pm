package Hadoop::Reducer;
use Moose::Role;

with 'Hadoop::Role::Emitter';

use IO::Handle;
use Params::Validate qw/validate_pos/;
use Hadoop::Reducer::Input;

with 'Hadoop::Role::Emitter';
requires qw/reduce/;

sub run {
    my $class = shift;
    my $self = $class->new;

    my $input = Hadoop::Reducer::Input->new(handle => \*STDIN);
    my $iter = $input->iterator;

    while ($iter->has_next) {
        my ($key, $values_iter) = $iter->next or last;
        eval {
            $self->reduce( $key => $values_iter );
        };
        if ($@) {
            warn $@;
        }
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

