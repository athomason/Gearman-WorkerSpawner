%define pkgname Gearman-WorkerSpawner
%define filelist %{pkgname}-%{version}-filelist
%define NVR %{pkgname}-%{version}-%{release}
%define maketest 1
%define VERSION %(grep 'VERSION =' lib/Gearman/WorkerSpawner.pm | perl -nle '/([0-9.]+)/ && print $1')

name:      perl-Gearman-WorkerSpawner
summary:   Gearman-WorkerSpawner - Subprocess manager for Gearman workers in a
version:   %{VERSION}
release:   1
vendor:    Adam Thomason, <athomason@sixapart.com>
packager:  Six Apart Ltd <cpan@sixapart.com>
license:   Artistic
group:     Applications/CPAN
url:       http://www.cpan.org
buildroot: %{_tmppath}/%{name}-%{version}-%(id -u -n)
buildarch: noarch
prefix:    %(echo %{_prefix})
source:    Gearman-WorkerSpawner-%{version}.tar.gz

autoreq:   off
requires:  perl(Carp), perl(Config), perl(Cwd), perl(Danga::Socket), perl(ExtUtils::MakeMaker), perl(Fcntl), perl(File::Find), perl(File::Path), perl(File::Spec), perl(FindBin), perl(Gearman::Client), perl(Gearman::Client::Async), perl(Gearman::Server), perl(IO::Handle), perl(IO::Socket::INET), perl(Module::Install::Base), perl(POSIX), perl(Storable), perl(base), perl(constant), perl(fields), perl(strict), perl(vars), perl(warnings), rpmlib(CompressedFileNames) <= 3.0.4-1, rpmlib(PayloadFilesHavePrefix) <= 4.0-1, rpmlib(VersionedDependencies) <= 3.0.3-1

autoprov:  off
provides:  perl(Gearman::WorkerSpawner), perl(Gearman::WorkerSpawner::BaseWorker), perl-Gearman-WorkerSpawner = 2.10-1

%description
None.

%prep
%setup -q -n %{pkgname}-%{version} 
chmod -R u+w %{_builddir}/%{pkgname}-%{version}

%build
grep -rsl '^#!.*perl' . |
grep -v '.bak$' |xargs --no-run-if-empty \
%__perl -MExtUtils::MakeMaker -e 'MY->fixin(@ARGV)'
CFLAGS="$RPM_OPT_FLAGS"
%{__perl} Makefile.PL `%{__perl} -MExtUtils::MakeMaker -e ' print qq|PREFIX=%{buildroot}%{_prefix}| if \$ExtUtils::MakeMaker::VERSION =~ /5\.9[1-6]|6\.0[0-5]/ '`
%{__make} 
%if %maketest
%{__make} test
%endif

%install
[ "%{buildroot}" != "/" ] && rm -rf %{buildroot}

%{makeinstall} `%{__perl} -MExtUtils::MakeMaker -e ' print \$ExtUtils::MakeMaker::VERSION <= 6.05 ? qq|PREFIX=%{buildroot}%{_prefix}| : qq|DESTDIR=%{buildroot}| '`

cmd=/usr/share/spec-helper/compress_files
[ -x $cmd ] || cmd=/usr/lib/rpm/brp-compress
[ -x $cmd ] && $cmd

# SuSE Linux
if [ -e /etc/SuSE-release -o -e /etc/UnitedLinux-release ]
then
    %{__mkdir_p} %{buildroot}/var/adm/perl-modules
    %{__cat} `find %{buildroot} -name "perllocal.pod"`  \
        | %{__sed} -e s+%{buildroot}++g                 \
        > %{buildroot}/var/adm/perl-modules/%{name}
fi

# remove special files
find %{buildroot} -name "perllocal.pod" \
    -o -name ".packlist"                \
    -o -name "*.bs"                     \
    |xargs -i rm -f {}

# no empty directories
find %{buildroot}%{_prefix}             \
    -type d -depth                      \
    -exec rmdir {} \; 2>/dev/null

%{__perl} -MFile::Find -le '
    find({ wanted => \&wanted, no_chdir => 1}, "%{buildroot}");
    print "%doc  inc Changes README";
    for my $x (sort @dirs, @files) {
        push @ret, $x unless indirs($x);
        }
    print join "\n", sort @ret;

    sub wanted {
        return if /auto$/;

        local $_ = $File::Find::name;
        my $f = $_; s|^\Q%{buildroot}\E||;
        return unless length;
        return $files[@files] = $_ if -f $f;

        $d = $_;
        /\Q$d\E/ && return for reverse sort @INC;
        $d =~ /\Q$_\E/ && return
            for qw|/etc %_prefix/man %_prefix/bin %_prefix/share|;

        $dirs[@dirs] = $_;
        }

    sub indirs {
        my $x = shift;
        $x =~ /^\Q$_\E\// && $x ne $_ && return 1 for @dirs;
        }
    ' > %filelist

[ -z %filelist ] && {
    echo "ERROR: empty %files listing"
    exit -1
    }

%clean
[ "%{buildroot}" != "/" ] && rm -rf %{buildroot}

%files -f %filelist
%defattr(-,root,root)

%changelog
* Tue Sep 1 2009 athomason@athomason-many
- Initial build.
