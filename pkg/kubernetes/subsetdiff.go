package kubernetes

import (
	"strings"

	"github.com/pkg/errors"

	"github.com/grafana/tanka/pkg/kubernetes/client"
	"github.com/grafana/tanka/pkg/kubernetes/manifest"
	"github.com/grafana/tanka/pkg/kubernetes/util"
)

type difference struct {
	name    string
	local   string // local (Jsonnet) state
	cluster string // live state in the cluster
}

// SubsetDiffer returns a implementation of Differ that computes the diff by
// comparing only the fields present in the desired state. This algorithm might
// miss information, but is all that's possible on cluster versions lower than
// 1.13.
func SubsetDiffer(c client.Client) Differ {
	return func(state manifest.List) (*string, error) {
		docs := []difference{}

		errCh := make(chan error)
		resultCh := make(chan difference)

		for _, rawShould := range state {
			go parallelSubsetDiff(c, rawShould, resultCh, errCh)
		}

		var lastErr error
		for i := 0; i < len(state); i++ {
			select {
			case d := <-resultCh:
				docs = append(docs, d)
			case err := <-errCh:
				lastErr = err
			}
		}
		close(resultCh)
		close(errCh)

		if lastErr != nil {
			return nil, errors.Wrap(lastErr, "calculating subset")
		}

		var diffs string
		for _, d := range docs {
			diffStr, err := util.DiffStr(d.name, d.local, d.cluster)
			if err != nil {
				return nil, errors.Wrap(err, "invoking diff")
			}
			if diffStr != "" {
				diffStr += "\n"
			}
			diffs += diffStr
		}
		diffs = strings.TrimSuffix(diffs, "\n")

		if diffs == "" {
			return nil, nil
		}

		return &diffs, nil
	}
}

func parallelSubsetDiff(c client.Client, should manifest.Manifest, r chan difference, e chan error) {
	diff, err := subsetDiff(c, should)
	if err != nil {
		e <- err
		return
	}
	r <- *diff
}

func subsetDiff(c client.Client, m manifest.Manifest) (*difference, error) {
	name := util.DiffName(m)

	// kubectl output -> current state
	res, err := c.Get(
		m.Metadata().Namespace(),
		m.Kind(),
		m.Metadata().Name(),
	)

	if _, ok := err.(client.ErrorNotFound); ok {
		res = map[string]interface{}{}
	} else if err != nil {
		return nil, errors.Wrap(err, "getting state from cluster")
	}

	local := m.String()
	cluster := manifest.Manifest(subset(m, res)).String()

	if string(cluster) == "{}\n" {
		cluster = ""
	}

	return &difference{
		name:    name,
		local:   string(cluster),
		cluster: string(local),
	}, nil
}

// subset removes all keys from is, that are not present in should.
// It makes is a subset of should.
// Kubernetes returns more keys than we can know about.
// This means, we need to remove all keys from the kubectl output, that are not present locally.
func subset(local, cluster map[string]interface{}) map[string]interface{} {
	if local["namespace"] != nil {
		cluster["namespace"] = local["namespace"]
	}

	// just ignore the apiVersion for now, too much bloat
	if local["apiVersion"] != nil && cluster["apiVersion"] != nil {
		cluster["apiVersion"] = local["apiVersion"]
	}

	for k, v := range cluster {
		if local[k] == nil {
			delete(cluster, k)
			continue
		}

		switch b := v.(type) {
		case map[string]interface{}:
			if a, ok := local[k].(map[string]interface{}); ok {
				cluster[k] = subset(a, b)
			}
		case []map[string]interface{}:
			for i := range b {
				if a, ok := local[k].([]map[string]interface{}); ok {
					b[i] = subset(a[i], b[i])
				}
			}
		case []interface{}:
			for i := range b {
				if a, ok := local[k].([]interface{}); ok {
					if i >= len(a) {
						// slice in config shorter than in live. Abort, as there are no entries to diff anymore
						break
					}

					// value not a dict, no recursion needed
					cShould, ok := a[i].(map[string]interface{})
					if !ok {
						continue
					}

					// value not a dict, no recursion needed
					cIs, ok := b[i].(map[string]interface{})
					if !ok {
						continue
					}
					b[i] = subset(cShould, cIs)
				}
			}
		}
	}
	return cluster
}
