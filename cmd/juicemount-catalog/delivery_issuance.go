package main

import (
    "context"
    "errors"
    "io"
    "strings"

    "github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

func runDeliveryIssuance(ctx context.Context, owner *catalog.LocalOwnerService, request catalog.AdminRequestV3, secretOut string, stdout, stderr io.Writer, owners *fileOwners, files cliFiles, knownSelector *string, outputCloseFailed *bool, fail func(bool) int) int {
	prior, found, err := owner.LookupIssuanceV3(ctx, request)
	if err != nil {
		return fail(false)
	}
	if found {
		*knownSelector = prior.Receipt.Result.Selector
		if writeJSON(stderr, prior) != nil {
			return fail(true)
		}
		return 0
	}
	writer := stdout
	var file *ownedFile
	if secretOut != "-" {
		raw, err := files.reserve(secretOut)
		if err != nil {
			return fail(false)
		}
		file = owners.track(raw)
		writer = file
	}
	result, err := owner.IssueCredentialV3(ctx, request)
	if err != nil {
		if file != nil {
			err = errors.Join(err, closeOutput(file))
		}
		*outputCloseFailed = errors.Is(err, errOutputClose)
		return fail(strings.Contains(err.Error(), "uncertain") || *outputCloseFailed)
	}
	*knownSelector = result.Receipt.Result.Selector
	if err = writeJSON(stderr, result); err != nil {
		if file != nil {
			err = errors.Join(err, closeOutput(file))
		}
		*outputCloseFailed = errors.Is(err, errOutputClose)
		return fail(true)
	}
	if len(result.Bearer) != 0 {
		err = writeBytes(writer, append(result.Bearer, '\n'))
	}
	if file != nil {
		if err == nil {
			err = finishOutput(file)
		} else {
			err = errors.Join(err, closeOutput(file))
		}
	}
	if err != nil {
		*outputCloseFailed = errors.Is(err, errOutputClose)
		return fail(true)
	}
	return 0
}
