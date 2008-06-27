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
    return 1 if $self->first;
    return unless defined $self->input_iter->input->next_key;
    return $self->input_iter->current_key eq $self->input_iter->input->next_key ? 1 : 0;
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

__PACKAGE__->meta->make_immutable;

1;

