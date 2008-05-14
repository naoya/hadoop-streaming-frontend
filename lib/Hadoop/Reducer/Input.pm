package Hadoop::Reducer::Input;
use Moose;
use Hadoop::Reducer::Input::Iterator;

has handle => (
    is       => 'ro',
    does     => 'FileHandle',
    required => 1,
);

has buffer => (
    is   => 'rw',
);

sub next_key {
    my $self = shift;
    my $line = $self->buffer ? $self->buffer : $self->next_line;
    return if not defined $line;
    my ($key, $value) = split /\t/, $line;
    return $key;
}

sub next_line {
    my $self = shift;
    return if $self->handle->eof;
    $self->buffer( $self->handle->getline );
    $self->buffer;
}

sub getline {
    my $self = shift;
    if (defined $self->buffer) {
        my $buf = $self->buffer;
        $self->buffer(undef);
        return $buf;
    } else {
        return $self->next_line;
    }
}

sub iterator {
    my $self = shift;
    Hadoop::Reducer::Input::Iterator->new( input => $self );
}

sub each {
    my $self = shift;
    my $line = $self->getline or return;
    chomp $line;
    split /\t/, $line;
}

1;
