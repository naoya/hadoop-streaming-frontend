package Hadoop::Reducer::Input::ValuesIterator;
use Moose;
with 'Hadoop::Role::Iterator';

has input_iter => (
    is       => 'ro',
    does     => 'Hadoop::Role::Iterator',
    required => 1,
);

has first => (
    is       => 'rw',
);

sub has_next {
    my $self = shift;
    $self->input_iter->input->next_key or return;
    $self->input_iter->current_key eq $self->input_iter->input->next_key;
}

sub next {
    my $self = shift;
    if (my $first = $self->first) {
        $self->first( undef );
        return $first;
    }
    my ($key, $value) = $self->input_iter->input->each;
    $value;
}

1;

