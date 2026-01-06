# OrchidDehydration

> *He’ll recover soon enough, when we soak him in water. It’s just like soaking dried mushrooms.*
>
> ——Three Body Problem

## Installation

```elixir
def deps do
  [
    {
      :orchid_dehydration,
      git: "https://github.com/SynapticStrings/OrchidDehydration.git"
    }
  ]
end
```

## Roadmap

- [ ] Param hydration/dehydration
  - [x] Implement Hook
  - [ ] Implement Repos
    - `:ets`
    - `:dets`
    - (Required discuss) remote(Node/HTTP)
- [ ] Recipe hydration/dehydration
  - [ ] pure recipe
  - [ ] with [OrchidSymbiont](https://github.com/SynapticStrings/OrchidSymbiont)
- [ ] Save & Resume
