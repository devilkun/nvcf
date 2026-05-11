"OCI image rules for packaging binaries into multi-arch containers."

load("//rules/oci/private:go.bzl", _go_oci_image = "go_oci_image")

go_oci_image = _go_oci_image
