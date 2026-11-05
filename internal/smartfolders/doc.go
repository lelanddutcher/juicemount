// Package smartfolders keeps server-written "smart folders" inside the
// JuiceFS volume: directories of relative symlinks that point back at where
// files really live, so every client (Mac NFS mounts, the WebDAV/S3
// gateways) sees them in Finder and in every app's file picker.
//
// The first view is <Root>/<top-level folder>/ (Root defaults to "Recents"):
// the most recently changed files under each top-level folder of the volume.
// A second root ("Collections", in the Manager) holds one view per
// collection, named by FolderNames; those views are removed (RemoveView)
// when their collection goes.
//
// The package knows nothing about Redis or the Manager. It has four parts:
//
//   - Tracker turns path-level change events into a bounded, age-limited,
//     per-top-level-folder list of recently changed files, and serialises
//     it so a restart does not empty the views.
//   - Names turns a recency list into display names; a name stays bound to
//     its target while the target stays listed.
//   - Reconciler keeps one view holding exactly the wanted links, by diff.
//     Everything it creates is recorded in an Ownership (persisted by the
//     caller) before it is created, and it never modifies anything else.
//   - FS is the volume it writes through: OSFS (a local mount, anchored with
//     os.Root) or jfsmeta.FS (a JuiceFS metadata client).
//
// Link targets are always relative ("../../Projects/Interviews/clip.mov"):
// each Mac mounts the volume at a different /Volumes path, and JuiceFS's own
// path resolver (WebDAV/S3 gateway) refuses absolute symlink targets but
// follows relative ones.
//
// Views are written by the server only. A file a user drops into a view is
// left alone and never adopted; writing into views from clients is not
// supported. Directory permissions are left at the defaults (0755).
package smartfolders
